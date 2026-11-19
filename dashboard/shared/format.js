// UTC, 24-hour formatting shared by the JavaScript variants. Never the browser's local time zone.
const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const p2 = (n) => String(n).padStart(2, "0");

export const hhmm = (ms) => { const d = new Date(ms); return `${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}`; };
export const hhmmss = (ms) => `${hhmm(ms)}:${p2(new Date(ms).getUTCSeconds())}`;
export const day = (ms) => { const d = new Date(ms); return `${DAYS[d.getUTCDay()]} ${p2(d.getUTCDate())} ${MONTHS[d.getUTCMonth()]}`; };
export const isoMinute = (ms) => new Date(ms).toISOString().slice(0, 16) + "Z";
export const verdictText = (v) => (v ? `needs human: ${v}` : "triage: investigating");
export const verdictClass = (v) => (v ? `sl-v-${v}` : "sl-v-none");
export const speedLabel = (m) => `${m < 60 ? m + " min" : m / 60 + " h"} / tick`;
export const fmtNum = (v) => (v == null ? "-" : Math.abs(v) >= 1e6 ? (v / 2 ** 20).toFixed(0) + " Mi" : String(+v.toPrecision(4)));

// hour ticks inside [from, to], every `stepH` hours, on whole UTC hours
export function hourTicks(from, to, stepH = 1) {
  const step = stepH * 3600e3, out = [];
  for (let x = Math.ceil(from / step) * step; x <= to; x += step) out.push(x);
  return out;
}

// the URL contract every variant reads: ?t=<ISO>&page=<n>&series=<id>&reveal=1&speed=<min>
export function readUrl(search, start, end, defaultSpeed) {
  const q = new URLSearchParams(search);
  const t = q.get("t") ? Date.parse(q.get("t")) : start;
  return {
    t: Math.min(end, Math.max(start, Number.isFinite(t) ? t : start)),
    page: q.get("page") ? Number(q.get("page")) : null,
    series: q.get("series"),
    reveal: q.get("reveal") === "1",
    speed: Number(q.get("speed")) || defaultSpeed,
  };
}
