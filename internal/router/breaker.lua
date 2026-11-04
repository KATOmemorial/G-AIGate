-- Validation and computation precede the commit section. Redis does NOT roll
-- back writes on a Lua error. Never add a fallible validation after that line.
local MAX = 9007199254740991
local function uint(s)
    if type(s) ~= 'string' or not string.match(s, '^%d+$') then return nil end
    local n = tonumber(s)
    if not n or n < 0 or n > MAX or n ~= math.floor(n) then return nil end
    return n
end
local function ident(s)
    return type(s) == 'string' and #s == 32 and string.match(s, '^[0-9a-f]+$')
end
local function dec(n) return string.format('%.0f', n) end
local function bad() return redis.error_reply('breaker invalid state or arithmetic') end
-- Redis ZSET score formatting may use exponent notation for exact integers.
local function scoreInteger(s)
    local n = tonumber(s)
    if not n or n < 0 or n > MAX or n ~= math.floor(n) then return nil end
    return n
end
local op, policy = ARGV[1], ARGV[2]
local window, minimum, ppm, cooldown = uint(ARGV[3]), uint(ARGV[4]), uint(ARGV[5]), uint(ARGV[6])
if not window or window < 1 or window > 3600000 or not minimum or minimum < 1 or minimum > 1000000 or
   not ppm or ppm < 1 or ppm > 1000000 or not cooldown or cooldown < 1 or cooldown > 3600000 then return bad() end
if op ~= 'inspect' and op ~= 'acquire' and op ~= 'complete' then return bad() end
local token, nextEpoch, lease = ARGV[7], ARGV[8], uint(ARGV[9])
local permitEpoch, kind, untilMS, outcome = ARGV[10], ARGV[11], uint(ARGV[12]), ARGV[13]
if op ~= 'inspect' and (not ident(token) or not ident(nextEpoch)) then return bad() end
if op == 'acquire' and (not lease or lease < 1 or lease > 86400000) then return bad() end
if op == 'complete' and (not ident(permitEpoch) or (kind ~= 'c' and kind ~= 'p') or not untilMS or untilMS < 1 or
   (outcome ~= 'success' and outcome ~= 'failure' and outcome ~= 'neutral')) then return bad() end
local tm = redis.call('TIME')
local seconds, micros = uint(tm[1]), uint(tm[2])
if not seconds or not micros or micros > 999999 or seconds > math.floor(MAX / 1000) then return bad() end
local now = seconds * 1000 + math.floor(micros / 1000)
if now > MAX - math.max(window, cooldown, lease or 0) then return bad() end
local types = {}
for i=1,3 do types[i] = redis.call('TYPE', KEYS[i]).ok end
if (types[1] ~= 'none' and types[1] ~= 'hash') or (types[2] ~= 'none' and types[2] ~= 'zset') or
   (types[3] ~= 'none' and types[3] ~= 'zset') then return bad() end
local missing = types[1] == 'none'
if missing and (types[2] ~= 'none' or types[3] ~= 'none') then return bad() end
local state, epoch, openUntil, probe, probeUntil = 'closed', nextEpoch, 0, '', 0
if not missing then
    local h = redis.call('HMGET', KEYS[1], 'schema', 'policy', 'state', 'epoch', 'open_until_ms', 'probe_token', 'probe_until_ms')
    state, epoch, openUntil, probe, probeUntil = h[3], h[4], uint(h[5]), h[6], uint(h[7])
    if h[1] ~= '1' or h[2] ~= policy or not ident(epoch) or not openUntil or not probeUntil or
       redis.call('PTTL', KEYS[1]) ~= -1 then return bad() end
    if state == 'closed' then
        if openUntil ~= 0 or probe ~= '' or probeUntil ~= 0 then return bad() end
    elseif state == 'open' then
        if probe ~= '' or probeUntil ~= 0 then return bad() end
    elseif state == 'half-open' then
        if openUntil ~= 0 or not ident(probe) or probeUntil < 1 then return bad() end
    else return bad() end
end
local pending = redis.call('ZRANGE', KEYS[3], 0, -1, 'WITHSCORES')
local active, maxPending, probeFound, retry = 0, 0, false, nil
local matching = false
local seenTokens = {}
for i=1,#pending,2 do
    local e,t,k = string.match(pending[i], '^([0-9a-f]+):([0-9a-f]+):([cp])$')
    local score = scoreInteger(pending[i+1])
    if not ident(e) or not ident(t) or not score or score < 1 then return bad() end
    if seenTokens[t] then return bad() end
    seenTokens[t] = true
    if e == epoch and ((k == 'c' and state ~= 'closed') or
       (k == 'p' and (state ~= 'half-open' or t ~= probe or score ~= probeUntil))) then return bad() end
    if score > now then
        active = active + 1
        maxPending = math.max(maxPending, score)
        if e == epoch and t == probe and k == 'p' and score == probeUntil then probeFound = true end
        if op == 'acquire' and t == token then
            if e ~= epoch or (k == 'p' and (state ~= 'half-open' or t ~= probe)) or (k == 'c' and state ~= 'closed') then return bad() end
            retry = {e,k,dec(score)}
        end
        if op == 'complete' and e == permitEpoch and t == token and k == kind and score == untilMS then matching = true end
    end
end
if active > 4096 or (state == 'half-open' and probeUntil > now and not probeFound) then return bad() end
local events = redis.call('ZRANGE', KEYS[2], 0, -1, 'WITHSCORES')
local n, failures = 0, 0
for i=1,#events,2 do
    local t,result = string.match(events[i], '^([0-9a-f]+):([sf])$')
    local score = scoreInteger(events[i+1])
    if not ident(t) or not score or score > now then return bad() end
    if score > now-window then
        n = n + 1
        if result == 'f' then failures = failures + 1 end
    end
end
-- Both cross products must remain exact, including the next sample.
if n >= math.floor(MAX / 1000000) then return bad() end
local eligible = (state == 'closed' and active < 4096) or (state == 'open' and now >= openUntil) or
                 (state == 'half-open' and now >= probeUntil)
if op == 'inspect' then return {state, eligible and 1 or 0} end
if op == 'acquire' and retry then return retry end
local writeHash, clearEvents, addEvent = false, false, nil
local member, deadline, response
if op == 'acquire' then
    if not eligible or active >= 4096 then return {} end
    if missing then writeHash = true end
    kind = 'c'
    deadline = now + lease
    if state ~= 'closed' then
        state, epoch, openUntil, probe, probeUntil = 'half-open', nextEpoch, 0, token, deadline
        kind, writeHash = 'p', true
    end
    member = epoch..':'..token..':'..kind
    maxPending = math.max(maxPending, deadline)
    response = {epoch,kind,dec(deadline)}
else
    if missing or not matching or permitEpoch ~= epoch or
       (kind == 'c' and state ~= 'closed') or (kind == 'p' and (state ~= 'half-open' or probe ~= token or probeUntil ~= untilMS)) then return {} end
    member = permitEpoch..':'..token..':'..kind
    if kind == 'p' then
        epoch, probe, probeUntil, writeHash = nextEpoch, '', 0, true
        if outcome == 'success' then state, openUntil, clearEvents = 'closed', 0, true
        else state, openUntil = 'open', now + (outcome == 'failure' and cooldown or 0) end
    elseif outcome ~= 'neutral' then
        n = n + 1
        if outcome == 'failure' then failures = failures + 1 end
        addEvent = token..':'..(outcome == 'failure' and 'f' or 's')
        if n >= minimum and failures * 1000000 >= n * ppm then
            state, epoch, openUntil, probe, probeUntil, writeHash = 'open', nextEpoch, now+cooldown, '', 0, true
        end
    end
    response = {}
end
-- Commit: all key types, inputs, current state and computed integers are valid.
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', dec(now))
if op == 'acquire' then redis.call('ZADD', KEYS[3], dec(deadline), member)
else redis.call('ZREM', KEYS[3], member) end
if maxPending > now and redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PEXPIREAT', KEYS[3], dec(maxPending)) end
if clearEvents then redis.call('DEL', KEYS[2])
elseif addEvent then
    redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', dec(now-window))
    redis.call('ZADD', KEYS[2], dec(now), addEvent)
    redis.call('PEXPIREAT', KEYS[2], dec(now+window))
end
if writeHash then
    redis.call('HSET', KEYS[1], 'schema', '1', 'policy', policy, 'state', state, 'epoch', epoch,
       'open_until_ms', dec(openUntil), 'probe_token', probe, 'probe_until_ms', dec(probeUntil))
end
return response
