import { apDayjsDuration, runsMetadataQueue } from '@activepieces/server-shared'
import { FastifyBaseLogger } from 'fastify'
import { workerMachine } from './machine'
import { workerRedisConnections } from './worker-redis'

type RunsMetadataQueueProducer = {
    init(): Promise<void>
}

export const workerRunsMetadataQueueProducer = (log: FastifyBaseLogger): RunsMetadataQueueProducer => ({
    async init(): Promise<void> {
        const connection = await workerRedisConnections.create()
        const settings = workerMachine.getSettings()
        
        const EIGHT_MINUTES_IN_MILLISECONDS = apDayjsDuration(8, 'minute').asMilliseconds()
        const FAILED_JOB_RETENTION_DAYS = apDayjsDuration(settings.REDIS_FAILED_JOB_RETENTION_DAYS, 'day').asSeconds()

        await runsMetadataQueue.init(
            connection,
            {
                attempts: 5,
                backoffDelay: EIGHT_MINUTES_IN_MILLISECONDS,
                failedJobRetentionDays: FAILED_JOB_RETENTION_DAYS,
                failedJobRetryCount: settings.REDIS_FAILED_JOB_RETENTION_MAX_COUNT,
                isOtelEnabled: settings.OTEL_ENABLED ?? false,
            },
        )
        log.info('[workerRunsMetadataQueueProducer#init] Runs metadata queue initialized')
    },
})

