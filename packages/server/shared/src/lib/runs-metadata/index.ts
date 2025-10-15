import { ApId, FlowRun, isNil } from '@activepieces/shared'
import { Static, Type } from '@sinclair/typebox'
import { Queue } from 'bullmq'
import { BullMQOtel } from 'bullmq-otel'
import { Redis } from 'ioredis'
import { QueueName } from '../job'

export const RunsMetadataJobData = Type.Object({
    runId: Type.String(),
})

export type RunsMetadataJobData = Static<typeof RunsMetadataJobData>

const UPSERT_RUN_METADATA_SCRIPT = `
local key = KEYS[1]
local runData = ARGV[1]
local ttl = tonumber(ARGV[2])

-- Get existing data
local existingData = redis.call('GET', key)

if existingData then
    -- Parse existing and new data
    local existing = cjson.decode(existingData)
    local new = cjson.decode(runData)
    
    -- Merge new fields into existing (new fields override)
    for k, v in pairs(new) do
        existing[k] = v
    end
    
    -- Store merged data
    redis.call('SET', key, cjson.encode(existing))
else
    -- Insert new data
    redis.call('SET', key, runData)
end

return redis.call('GET', key)
`

export type RunsMetadataUpsertData = Partial<FlowRun> & {
    id: ApId
}

export type RunsMetadataJobOptions = {
    attempts: number
    backoffDelay: number
    failedJobRetentionDays: number
    failedJobRetryCount: number
    isOtelEnabled: boolean
}

let runsMetadataQueueInstance: Queue<RunsMetadataJobData> | undefined = undefined
let redisConnectionInstance: Redis | undefined = undefined

export const runsMetadataQueue = {
    async init(
        redisConnection: Redis,
        options: RunsMetadataJobOptions,
    ): Promise<void> {
        if (!isNil(runsMetadataQueueInstance)) {
            return
        }

        redisConnectionInstance = redisConnection

        runsMetadataQueueInstance = new Queue<RunsMetadataJobData>(QueueName.RUNS_METADATA, {
            connection: redisConnection,
            telemetry: options.isOtelEnabled ? new BullMQOtel(QueueName.RUNS_METADATA) : undefined,
            defaultJobOptions: {
                attempts: options.attempts,
                backoff: {
                    type: 'exponential',
                    delay: options.backoffDelay,
                },
                removeOnComplete: true,
                removeOnFail: {
                    age: options.failedJobRetentionDays,
                    count: options.failedJobRetryCount,
                },
            },
        })

        await runsMetadataQueueInstance.waitUntilReady()
    },

    async add(params: RunsMetadataUpsertData): Promise<void> {
        if (isNil(runsMetadataQueueInstance)) {
            throw new Error('Runs metadata queue not initialized. Call runsMetadataQueue.init() first.')
        }

        if (isNil(redisConnectionInstance)) {
            throw new Error('Redis connection not initialized.')
        }

        // Upsert run metadata in Redis using Lua script
        const key = getRunsMetadataKey(params.id)
        const runData = JSON.stringify(params)

        await redisConnectionInstance.eval(
            UPSERT_RUN_METADATA_SCRIPT,
            1,
            key,
            runData,
        )

        // Add job with only runId to keep job data minimal
        await runsMetadataQueueInstance.add(
            'update-run-metadata',
            { runId: params.id },
            { deduplication: { id: params.id } },
        )
    },

    get(): Queue<RunsMetadataJobData> {
        if (isNil(runsMetadataQueueInstance)) {
            throw new Error('Runs metadata queue not initialized. Call runsMetadataQueue.init() first.')
        }
        return runsMetadataQueueInstance
    },

    async getRunMetadata(runId: ApId): Promise<RunsMetadataUpsertData | null> {
        if (isNil(redisConnectionInstance)) {
            throw new Error('Redis connection not initialized.')
        }

        const key = getRunsMetadataKey(runId)
        const data = await redisConnectionInstance.get(key)
        
        if (isNil(data)) {
            return null
        }

        return JSON.parse(data) as RunsMetadataUpsertData
    },

    async deleteRunMetadata(runId: ApId): Promise<void> {
        if (isNil(redisConnectionInstance)) {
            throw new Error('Redis connection not initialized.')
        }

        const key = getRunsMetadataKey(runId)
        await redisConnectionInstance.del(key)
    },
}

const getRunsMetadataKey = (runId: ApId): string => `runs-metadata:${runId}`


