import { AppSystemProp, QueueName, RunsMetadataJobData, runsMetadataQueue } from '@activepieces/server-shared'
import { isNil } from '@activepieces/shared'
import { Worker } from 'bullmq'
import { BullMQOtel } from 'bullmq-otel'
import { FastifyBaseLogger } from 'fastify'
import { redisConnections } from '../../../database/redis-connections'
import { system } from '../../../helper/system/system'
import { flowRunRepo } from '../flow-run-service'

let runsMetadataWorker: Worker<RunsMetadataJobData> | undefined = undefined

type RunsMetadataQueueConsumer = {
    init(): Promise<void>
    close(): Promise<void>
    run(): Promise<void>
}

export const runsMetadataQueueConsumer = (log: FastifyBaseLogger): RunsMetadataQueueConsumer => ({
    async init(): Promise<void> {
        runsMetadataWorker = await ensureWorkerExists(log)
        log.info('[runsMetadataQueueConsumer#init] Runs metadata worker initialized')
    },
    
    async close(): Promise<void> {
        if (runsMetadataWorker) {
            await runsMetadataWorker.close()
            runsMetadataWorker = undefined
        }
    },
    
    async run(): Promise<void> {
        if (runsMetadataWorker) {
            await runsMetadataWorker.run()
        }
    },
})

async function ensureWorkerExists(log: FastifyBaseLogger): Promise<Worker<RunsMetadataJobData>> {
    if (!isNil(runsMetadataWorker)) {
        return runsMetadataWorker
    }
    
    const isOtelEnabled = system.getBoolean(AppSystemProp.OTEL_ENABLED)
    
    const worker = new Worker<RunsMetadataJobData>(
        QueueName.RUNS_METADATA,
        async (job) => {
            await runsMetadataQueue.get().removeDeduplicationKey(job.data.runId)
            log.info({
                message: 'Processing runs metadata job',
                jobId: job.id,
                runId: job.data.runId,
            })
            
            try {
                const runMetadata = await runsMetadataQueue.getRunMetadata(job.data.runId)

                if (isNil(runMetadata)) {
                    log.warn({
                        message: 'No metadata found for run',
                        jobId: job.id,
                        runId: job.data.runId,
                    })
                    return
                }
                
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                await flowRunRepo().upsert(runMetadata as any, ['id'])

                const deleteResult = await runsMetadataQueue.deleteRunMetadataIfUnchanged(
                    job.data.runId,
                    runMetadata.updated,
                )
                    
                if (deleteResult.deleted) {
                    log.info({
                        message: 'Deleted runs metadata from Redis',
                        jobId: job.id,
                        runId: job.data.runId,
                    })
                }
                else {
                    log.info({
                        message: 'Kept runs metadata in Redis',
                        jobId: job.id,
                        runId: job.data.runId,
                        reason: deleteResult.reason,
                    })
                }
                
            }
            catch (error) {
                log.error({
                    message: 'Error processing runs metadata job',
                    jobId: job.id,
                    runId: job.data.runId,
                    error,
                })
                throw error
            }
        },
        {
            connection: await redisConnections.create(),
            telemetry: isOtelEnabled ? new BullMQOtel(QueueName.RUNS_METADATA) : undefined,
            concurrency: 10,
            autorun: false,
        },
    )

    await worker.waitUntilReady()
    return worker
}

