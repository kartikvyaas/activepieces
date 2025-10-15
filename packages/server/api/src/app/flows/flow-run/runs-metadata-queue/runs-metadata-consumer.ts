import { AppSystemProp, QueueName, RunsMetadataJobData, runsMetadataQueue } from '@activepieces/server-shared'
import { isNil } from '@activepieces/shared'
import { Worker } from 'bullmq'
import { BullMQOtel } from 'bullmq-otel'
import { FastifyBaseLogger } from 'fastify'
import { redisConnections } from '../../../database/redis-connections'
import { system } from '../../../helper/system/system'

let runsMetadataWorker: Worker<RunsMetadataJobData> | undefined = undefined

export const runsMetadataQueueConsumer = (log: FastifyBaseLogger) => ({
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
            
            // TODO: Implementation will fetch the latest run from a separate store
            // by the runId from job data and then update the database
            // This is out of scope for now
            
            log.info({
                message: 'Runs metadata job processed successfully',
                jobId: job.id,
                runId: job.data.runId,
            })
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

