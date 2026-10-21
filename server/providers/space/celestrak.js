import path from 'node:path';
import { promises as fsp } from 'node:fs';
import { celestrakTleUrl } from '../../../src/data/spaceProviderRequests.js';
import { readResponseTextCapped } from '../common/http.js';

/**
 * Vite plugin: CelesTrak TLE proxy.
 *
 * CelesTrak does not send CORS headers, so this middleware fetches
 * satellite TLE data server-side and forwards it to the browser.
 * Upstream URL: https://celestrak.org/NORAD/elements/gp.php
 *
 * @returns {import('vite').Plugin}
 */
/**
 * CelesTrak GP/TLE proxy with a memory + disk cache.
 * Upstream: https://celestrak.org/NORAD/elements/gp.php?GROUP=<group>&FORMAT=tle
 * CelesTrak asks clients not to re-fetch GP data more than ~every 2 h and
 * throttles offenders; every dev reload used to refetch every group. Cache TTL
 * 6 h; on upstream failure the freshest stale copy is served (a stale TLE
 * beats an empty satellites layer). Pattern mirrors openSkyProxy's
 * cache+serve-stale. Adapted from skylight's TleStore (MIT).
 */
export function celestrakProxy() {
  const TLE_TTL_MS = 6 * 3600_000;
  const CACHE_DIR = path.join(process.cwd(), '.gev-cache');
  const mem = new Map(); // group -> { at: epochMs, body: string }
  const inflight = new Map(); // group -> Promise<{at, body}|null>
  const retryAfter = new Map(); // group -> epochMs after failed refresh
  const FAILED_REFRESH_BACKOFF_MS = 15 * 60_000;

  const diskPath = (group) => path.join(CACHE_DIR, `celestrak-${group}.json`);

  async function readDisk(group) {
    try {
      const parsed = JSON.parse(await fsp.readFile(diskPath(group), 'utf8'));
      if (typeof parsed?.body === 'string' && Number.isFinite(parsed?.at))
        return parsed;
    } catch {
      /* no disk cache yet */
    }
    return null;
  }

  async function writeDisk(group, entry) {
    try {
      await fsp.mkdir(CACHE_DIR, { recursive: true });
      await fsp.writeFile(diskPath(group), JSON.stringify(entry), 'utf8');
    } catch (err) {
      console.warn('[celestrak-proxy] cache write failed');
    }
  }

  /** The largest group (`active`) measures ~2.7 MB; this bounds a runaway body. */
  const CELESTRAK_MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

  async function fetchUpstream(group) {
    const url = celestrakTleUrl(group);
    const signal = AbortSignal.timeout(20000);
    const res = await fetch(url.toString(), {
      signal,
      // CelesTrak 403s bulk groups (e.g. `active`) unless the request carries a
      // descriptive User-Agent with a contact point.
      headers: {
        'User-Agent':
          'gods-eye-view-celestrak-proxy/1.0 (+https://github.com/bilawalsidhu/gods-eye-view)',
      },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await readResponseTextCapped(
      res,
      CELESTRAK_MAX_RESPONSE_BYTES,
      signal,
    );
    // An upstream error page parses to zero TLEs — treat as failure, keep cache.
    if (!/^1 /m.test(body)) throw new Error('no TLE lines in response');
    return { at: Date.now(), body, source: 'CELESTRAK' };
  }

  // Independent static mirror for groups confirmed published by Satvisor.
  // Read-only fallback; do not retry the unavailable origin on every request.
  const MIRROR_GROUPS = new Set([
    'stations', 'visual', 'active', 'last-30-days', 'analyst',
    'gps-ops', 'glo-ops', 'galileo', 'geo',
    'starlink', 'oneweb', 'kuiper', 'qianfan', 'hulianwang',
    'fengyun-1c-debris', 'cosmos-2251-debris', 'iridium-33-debris',
    'cosmos-1408-debris', 'decaying', 'gpz', 'gpz-plus',
  ]);
  async function fetchMirror(group) {
    if (!MIRROR_GROUPS.has(group)) return null;
    const url = `https://raw.githubusercontent.com/satvisorcom/satvisor-data/master/celestrak/tle/${group}.tle`;
    const signal = AbortSignal.timeout(12000);
    const res = await fetch(url, { signal });
    if (!res.ok) throw new Error(`mirror HTTP ${res.status}`);
    const body = await readResponseTextCapped(res, CELESTRAK_MAX_RESPONSE_BYTES, signal);
    if (!/^1 /m.test(body) || !/^2 /m.test(body)) throw new Error('invalid mirror TLE');
    return { at: Date.now(), body, source: 'MIRROR' };
  }

  const installMiddleware = (server) => {
    server.middlewares.use('/api/celestrak', async (req, res) => {
      const group = String(req.url || '')
        .replace(/^\//, '')
        .split('?')[0];
      if (!/^[a-z0-9-]+$/i.test(group)) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('invalid group');
        return;
      }
      const send = (status, body, cacheStatus) => {
        // Guard against a double-send (e.g. a throw AFTER a response already
        // went out routing into the catch's send): writeHead after headersSent
        // throws "Cannot set headers after they are sent".
        if (res.headersSent) return;
        res.writeHead(status, {
          'Content-Type': 'text/plain',
          'x-tle-cache': cacheStatus,
        });
        res.end(body);
      };
      try {
        const now = Date.now();
        let entry = mem.get(group);
        if (!entry) {
          entry = await readDisk(group);
          if (entry) mem.set(group, entry);
        }
        if (entry && now - entry.at < TLE_TTL_MS) {
          send(200, entry.body, entry.source === 'MIRROR' ? 'MIRROR-HIT' : 'HIT');
          return;
        }
        // Respect a cooldown after upstream errors; do not hammer CelesTrak.
        if (!inflight.has(group) && now < (retryAfter.get(group) || 0)) {
          if (entry) send(200, entry.body, 'STALE-ERROR');
          else send(502, 'celestrak temporarily unavailable; retry later', 'BACKOFF');
          return;
        }
        // Stale or missing → refresh, single-flight per group.
        if (!inflight.has(group)) {
          inflight.set(
            group,
            fetchUpstream(group)
              .catch(async (error) => {
                if (!MIRROR_GROUPS.has(group)) throw error;
                try {
                  const mirror = await fetchMirror(group);
                  console.info(`[celestrak-proxy] ${group}: mirror fallback in use`);
                  return mirror;
                } catch (mirrorError) {
                  const reason = String(mirrorError?.cause?.code || mirrorError?.message || 'unknown').slice(0, 100);
                  console.warn(`[celestrak-proxy] ${group}: mirror also unavailable (${reason})`);
                  throw error;
                }
              })
              .then(async (fresh) => {
                retryAfter.delete(group);
                mem.set(group, fresh);
                await writeDisk(group, fresh);
                return fresh;
              })
              .catch((err) => {
                retryAfter.set(group, Date.now() + FAILED_REFRESH_BACKOFF_MS);
                const cause = err?.cause;
                const causeCode = String(cause?.code || 'unknown').slice(0, 50);
                const causeMessage = String(cause?.message || '').replace(/[\r\n\t]/g, ' ').slice(0, 120);
                const kind = String(err?.name || 'Error').slice(0, 50);
                const detail = String(err?.message || 'unknown failure')
                  .replace(/[\r\n\t]/g, ' ')
                  .slice(0, 180);
                console.warn(
                  `[celestrak-proxy] refresh failed (${kind}: ${detail}; cause=${causeCode}: ${causeMessage}) — backoff 15m, serving cache if any`,
                );
                return null;
              })
              .finally(() => inflight.delete(group)),
          );
        }
        const fresh = await inflight.get(group);
        if (fresh) {
          send(200, fresh.body, fresh.source === 'MIRROR' ? 'MIRROR' : 'MISS');
        } else if (entry) {
          send(200, entry.body, 'STALE-ERROR'); // upstream down — stale beats empty
        } else {
          send(502, 'celestrak fetch failed and no cache available', 'NONE');
        }
      } catch (err) {
        console.error('[celestrak-proxy] request failed');
        send(500, 'celestrak proxy error', 'ERROR');
      }
    });
  };
  return {
    name: 'celestrak-proxy',
    configureServer: installMiddleware,
    configurePreviewServer: installMiddleware,
  };
}
