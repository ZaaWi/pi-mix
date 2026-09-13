import React, { useEffect, useRef, useState } from 'react';
import { Bell, X } from 'lucide-react';
import { Message } from '../types';

const POLL_MS = 10000;
const FRESH_MS = 4000;
const MAX_VISIBLE = 6;

function timeAgo(ts: number): string {
  const s = Math.floor(Date.now() / 1000) - ts;
  if (s < 60) return 'just now';
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  const d = Math.floor(s / 86400);
  if (d < 7) return `${d}d ago`;
  return new Date(ts * 1000).toLocaleDateString([], { month: 'short', day: 'numeric' });
}

function formatTimestamp(ts: number): string {
  return new Date(ts * 1000).toLocaleString([], {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

const keyOf = (m: Message) => `${m.timestamp}:${m.title}`;

export const MessagesPanel: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState<Message | null>(null);
  const [fresh, setFresh] = useState<Set<number>>(new Set());
  const prevKeys = useRef<Set<string>>(new Set());

  useEffect(() => {
    const controller = new AbortController();
    const load = async (initial: boolean) => {
      try {
        const res = await fetch('/api/messages?limit=15', { signal: controller.signal });
        if (!res.ok) throw new Error('messages unavailable');
        const json = await res.json();
        const list: Message[] = Array.isArray(json.messages) ? json.messages : [];
        setMessages(list);
        setError('');

        if (!initial) {
          const freshTs = new Set(
            list.filter(m => !prevKeys.current.has(keyOf(m))).map(m => m.timestamp)
          );
          if (freshTs.size > 0) {
            setFresh(freshTs);
            setTimeout(() => setFresh(new Set()), FRESH_MS);
          }
        }
        prevKeys.current = new Set(list.map(keyOf));
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : 'messages unavailable');
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };

    load(true);
    const interval = setInterval(() => load(false), POLL_MS);
    return () => { controller.abort(); clearInterval(interval); };
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setSelected(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const visible = messages.slice(0, MAX_VISIBLE);

  return (
    <div className="glass glass-panel messages-container">
      <div className="messages-head">
        <div className="sensor-header">
          <Bell color="#fbbf24" />
          <span>Messages</span>
        </div>
        <div className="msg-live-area">
          {error ? (
            <span className="msg-live-chip msg-live-off">OFFLINE</span>
          ) : (
            <>
              <span className="msg-live-chip">
                <span className="msg-live-dot"></span>
                LIVE
              </span>
              {fresh.size > 0 && (
                <span className="msg-new-count" key={Date.now()}>
                  {fresh.size > 1 ? `+${fresh.size}` : '+1'}
                </span>
              )}
            </>
          )}
        </div>
      </div>

      {error ? (
        <div role="alert" className="msg-empty">
          {error}
        </div>
      ) : loading && messages.length === 0 ? (
        <div className="msg-empty">Loading messages...</div>
      ) : messages.length === 0 ? (
        <div className="msg-empty">No messages yet.</div>
      ) : (
        <ul className="msg-list">
          {visible.map(m => (
            <li key={`${m.timestamp}:${m.title}`}>
              <button
                type="button"
                className={`msg-item ${fresh.has(m.timestamp) ? 'msg-item-fresh' : ''}`}
                onClick={() => setSelected(m)}
              >
                <span className="msg-dot"></span>
                <span className="msg-body">
                  <span className="msg-title">{m.title}</span>
                  <span className="msg-text">{m.message}</span>
                </span>
                <time className="msg-time" dateTime={new Date(m.timestamp * 1000).toISOString()}>
                  {timeAgo(m.timestamp)}
                </time>
              </button>
            </li>
          ))}
        </ul>
      )}

      {selected && (
        <div className="msg-overlay" onClick={() => setSelected(null)}>
          <div
            className="msg-modal glass glass-panel"
            onClick={e => e.stopPropagation()}
            role="dialog"
            aria-modal="true"
            aria-label={selected.title}
          >
            <div className="msg-modal-head">
              <span className="msg-modal-icon"><Bell size={20} color="#fbbf24" /></span>
              <h3>{selected.title}</h3>
              <button
                type="button"
                className="msg-modal-close"
                onClick={() => setSelected(null)}
                aria-label="Close"
              >
                <X size={18} />
              </button>
            </div>
            <p className="msg-modal-time">{formatTimestamp(selected.timestamp)}</p>
            <p className="msg-modal-message">{selected.message}</p>
            {selected.desc && <p className="msg-modal-desc">{selected.desc}</p>}
          </div>
        </div>
      )}
    </div>
  );
};
