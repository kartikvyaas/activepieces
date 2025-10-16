import { ApId, FlowRun, isNil } from '@activepieces/shared'
import { Static, Type } from '@sinclair/typebox'
import { Queue } from 'bullmq'
import { BullMQOtel } from 'bullmq-otel'
import { Redis } from 'ioredis'

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

const DELETE_IF_UNCHANGED_SCRIPT = `
local key = KEYS[1]
local expectedUpdatedAt = ARGV[1]

-- Get current data
local existingData = redis.call('GET', key)

if not existingData then
    -- Key doesn't exist, return -1
    return -1
end

-- Parse the data
local data = cjson.decode(existingData)

-- Check if updatedAt matches
if tostring(data.updated) == tostring(expectedUpdatedAt) then
    -- updatedAt matches, safe to delete
    redis.call('DEL', key)
    return 1
else
    -- updatedAt has changed, don't delete
    return 0
end
`

export type RunsMetadataUpsertData = Partial<FlowRun> & {
    id: ApId
    updated: string
}

export enum RunsMetadataDeleteFailureReason {
    UPDATED_AT_CHANGED = 'UPDATED_AT_CHANGED',
    KEY_NOT_FOUND = 'KEY_NOT_FOUND',
}

export type RunsMetadataJobOptions = {
    attempts: number
    backoffDelay: number
    failedJobRetentionDays: number
    failedJobRetryCount: number
    isOtelEnabled: boolean
}

export const RUNS_METADATA_QUEUE_NAME = 'runsMetadata'

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

        runsMetadataQueueInstance = new Queue<RunsMetadataJobData>(RUNS_METADATA_QUEUE_NAME, {
            connection: redisConnection,
            telemetry: options.isOtelEnabled ? new BullMQOtel(RUNS_METADATA_QUEUE_NAME) : undefined,
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

        const key = this.getRunsMetadataKey(params.id)
        const runData = JSON.stringify(params)

        await redisConnectionInstance.eval(
            UPSERT_RUN_METADATA_SCRIPT,
            1,
            key,
            runData,
        )

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

        const key = this.getRunsMetadataKey(runId)
        const data = await redisConnectionInstance.get(key)

        if (isNil(data)) {
            return null
        }

        const parsed = JSON.parse(data) as RunsMetadataUpsertData
        
        // Fix tags if it was converted to an object by Lua cjson (empty arrays become empty objects)
        if (parsed.tags !== undefined && !Array.isArray(parsed.tags)) {
            parsed.tags = []
        }
        
        return parsed
    },

    async deleteRunMetadata(runId: ApId): Promise<void> {
        if (isNil(redisConnectionInstance)) {
            throw new Error('Redis connection not initialized.')
        }

        const key = this.getRunsMetadataKey(runId)
        await redisConnectionInstance.del(key)
    },

    async deleteRunMetadataIfUnchanged(runId: ApId, expectedUpdatedAt: string): Promise<{ deleted: true } | { deleted: false, reason: RunsMetadataDeleteFailureReason }> {
        if (isNil(redisConnectionInstance)) {
            throw new Error('Redis connection not initialized.')
        }

        const key = this.getRunsMetadataKey(runId)
        const result = await redisConnectionInstance.eval(
            DELETE_IF_UNCHANGED_SCRIPT,
            1,
            key,
            new Date(expectedUpdatedAt).toISOString(),
        ) as number

        if (result === 1) {
            return { deleted: true }
        }
        else if (result === 0) {
            return { deleted: false, reason: RunsMetadataDeleteFailureReason.UPDATED_AT_CHANGED }
        }
        else {
            return { deleted: false, reason: RunsMetadataDeleteFailureReason.KEY_NOT_FOUND }
        }
    },

    getRunsMetadataKey: (runId: ApId): string => `runs-metadata:${runId}`,
}


