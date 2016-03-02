-- ZRANGEBYSCORESTORE <key> <dst> <min> <max>
local tbl = redis.call('ZRANGEBYSCORE', KEYS[1],  ARGV[1], ARGV[2], 'WITHSCORES')

for i,_ in ipairs(tbl) do
    if i % 2 == 1 then
        tbl[i], tbl[i+1] =tbl[i+1], tbl[i]
    end
end
redis.call('ZADD', KEYS[2], unpack(tbl))
