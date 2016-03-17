--[[
intersect a list of token keys, calculating "idf" so that for each one, the score would be tf-idf
]]

local tbl = {}
local numKeys = table.getn(ARGV)

-- we call ZCARD to get the number of docs the word appears in 
for i,elem in ipairs(ARGV) do
    tbl[i] = ARGV[i]
    tbl[i+numKeys+1] = math.log(1000000/(1+redis.call('ZCARD', elem)))
end
tbl[numKeys+1] = 'WEIGHTS'
table.insert(tbl, 'AGGREGATE')
table.insert(tbl, 'SUM')

local rc = redis.call('ZINTERSTORE', KEYS[1], numKeys, unpack(tbl))
redis.expire(KEYS[1], 60)
return redis.status_reply(rc)

