import { apAxios, AppSystemProp, RUNS_METADATA_QUEUE_NAME, RunsMetadataJobData, runsMetadataQueue } from '@activepieces/server-shared'
import { assertNotNullOrUndefined, FlowRunStatus, isNil, PauseType } from '@activepieces/shared'
import { Worker } from 'bullmq'
import { BullMQOtel } from 'bullmq-otel'
import { FastifyBaseLogger } from 'fastify'
import { redisConnections } from '../../../database/redis-connections'
import { domainHelper } from '../../../ee/custom-domains/domain-helper'
import { system } from '../../../helper/system/system'
import { projectService } from '../../../project/project-service'
import { flowRunRepo, flowRunService } from '../flow-run-service'
import { flowRunSideEffects } from '../flow-run-side-effects'

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
            log.info('[runsMetadataQueueConsumer#run] Running runs metadata worker')
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
        RUNS_METADATA_QUEUE_NAME,
        async (job) => {
            log.info({
                message: 'Processing runs metadata job',
                jobId: job.id,
                runId: job.data.runId,
            })
            await runsMetadataQueue.get().removeDeduplicationKey(job.data.runId)
            
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
                await flowRunRepo().save(runMetadata)

                const flowRun = await flowRunService(log).getOneOrThrow({ id: job.data.runId, projectId: runMetadata.projectId })
                
                const shouldMarkParentAsFailed = flowRun.failParentOnFailure && !isNil(flowRun.parentRunId) && ![FlowRunStatus.SUCCEEDED, FlowRunStatus.RUNNING, FlowRunStatus.PAUSED, FlowRunStatus.QUEUED].includes(flowRun.status)
                if (shouldMarkParentAsFailed) {
                    const platformId = await projectService.getPlatformId(flowRun.projectId)
                    await markParentRunAsFailed({
                        parentRunId: flowRun.parentRunId!,
                        childRunId: flowRun.id,
                        projectId: flowRun.projectId,
                        platformId,
                    })
                }

                if (!isNil(runMetadata.finishTime)) {
                    await flowRunSideEffects(log).onFinish(flowRun)
                }

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
            telemetry: isOtelEnabled ? new BullMQOtel(RUNS_METADATA_QUEUE_NAME) : undefined,
            concurrency: 10,
            autorun: false,
        },
    )

    await worker.waitUntilReady()
    return worker
}

async function markParentRunAsFailed({
    parentRunId,
    childRunId,
    projectId,
    platformId,
}: MarkParentRunAsFailedParams): Promise<void> {
    const flowRun = await flowRunRepo().findOneByOrFail({
        id: parentRunId,
    })

    const requestId = flowRun.pauseMetadata?.type === PauseType.WEBHOOK ? flowRun.pauseMetadata?.requestId : undefined
    assertNotNullOrUndefined(requestId, 'Parent run has no request id')

    const callbackUrl = await domainHelper.getPublicApiUrl({ path: `/v1/flow-runs/${parentRunId}/requests/${requestId}`, platformId })
    const childRunUrl = await domainHelper.getPublicUrl({ path: `/projects/${projectId}/runs/${childRunId}`, platformId })
    await apAxios.post(callbackUrl, {
        status: 'error',
        data: {
            message: 'Subflow execution failed',
            link: childRunUrl,
        },
    })
}

type MarkParentRunAsFailedParams = {
    parentRunId: string
    childRunId: string
    projectId: string
    platformId: string
}
