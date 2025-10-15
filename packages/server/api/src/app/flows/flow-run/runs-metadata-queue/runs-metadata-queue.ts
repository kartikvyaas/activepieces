import { apDayjsDuration, AppSystemProp, runsMetadataQueue } from '@activepieces/server-shared'
import { FastifyBaseLogger } from 'fastify'
import { redisConnections } from '../../../database/redis-connections'
import { system } from '../../../helper/system/system'

const EIGHT_MINUTES_IN_MILLISECONDS = apDayjsDuration(8, 'minute').asMilliseconds()
const REDIS_FAILED_JOB_RETENTION_DAYS = apDayjsDuration(system.getNumberOrThrow(AppSystemProp.REDIS_FAILED_JOB_RETENTION_DAYS), 'day').asSeconds()
const REDIS_FAILED_JOB_RETRY_COUNT = system.getNumberOrThrow(AppSystemProp.REDIS_FAILED_JOB_RETENTION_MAX_COUNT)

type RunsMetadataQueueProducer = {
    init(): Promise<void>
}

export const runsMetadataQueueProducer = (log: FastifyBaseLogger): RunsMetadataQueueProducer => ({
    async init(): Promise<void> {
        const connection = await redisConnections.create()
        
        await runsMetadataQueue.init(
            connection,
            {
                attempts: 5,
                backoffDelay: EIGHT_MINUTES_IN_MILLISECONDS,
                failedJobRetentionDays: REDIS_FAILED_JOB_RETENTION_DAYS,
                failedJobRetryCount: REDIS_FAILED_JOB_RETRY_COUNT,
                isOtelEnabled: system.getBoolean(AppSystemProp.OTEL_ENABLED) ?? false,
            },
        )
        log.info('[runsMetadataQueueProducer#init] Runs metadata queue initialized')
    },
})

