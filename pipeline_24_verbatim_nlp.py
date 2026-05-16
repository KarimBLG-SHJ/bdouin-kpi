#!/usr/bin/env python3
"""
pipeline_24_verbatim_nlp.py — NLP sur verbatims/commentaires/sujets

Analyse sentiment + thèmes + intention via Claude Haiku 4.5 et stocke dans
audience.verbatim_nlp. Idempotent (UNIQUE entity_type+entity_id).

Sources (CLI --source) :
  contest   : audience.signals signal_type='verbatim' (Foulane + Hoopow)
  instagram : audience.signals signal_type='comment' source='instagram'
  campaigns : clean.ml_campaigns subject + name

Usage :
  python3 pipeline_24_verbatim_nlp.py --source contest [--limit N] [--dry-run]
"""

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

import psycopg2
from psycopg2.extras import execute_values
from anthropic import AsyncAnthropic, APIStatusError, RateLimitError

DB_URL = os.environ.get(
    'BDOUIN_DB',
    'postgresql://postgres:FnaPWAOtCnCLDJJbcRgkOJRvESnUHUVH@shortline.proxy.rlwy.net:33685/railway',
)
MODEL = 'claude-haiku-4-5-20251001'
CONCURRENCY = 8

THEMES = [
    'gratitude',
    'praise_product',
    'praise_team',
    'request_concours',
    'request_new_title',
    'request_product',
    'request_translation',
    'complaint_price',
    'complaint_shipping',
    'complaint_quality',
    'complaint_availability',
    'suggestion_content',
    'family_identification',
    'religious_values',
    'support_brand',
    'question_practical',
    'unsubscribe_signal',
    'spam_or_noise',
    'other',
]

INTENTIONS = [
    'praise',
    'gratitude',
    'request',
    'complaint',
    'suggestion',
    'question',
    'testimony',
    'other',
]

SYSTEM_PROMPT = f"""Tu es un classifieur de verbatims pour BDouin Edition, éditeur français de BD jeunesse à valeurs musulmanes (séries Foulane, Hoopow, Awlad School, Halua).

Le public est francophone (FR/BE/CH/Maghreb/Afrique francophone), souvent multilingue (français + arabe translittéré comme "salam aleykoum", "barakAllahou fikoum"). Beaucoup d'emojis. Verbatims sources : concours, commentaires Instagram, sujets de newsletter.

Pour CHAQUE texte fourni, retourne UNIQUEMENT un appel à l'outil submit_analysis avec :
- lang : 'fr' | 'ar' (arabe translittéré ou script) | 'en' | 'mixed' | 'unknown' (cas emoji seul / inintelligible)
- sentiment : 'positive' | 'neutral' | 'negative'
- sentiment_score : -1.0 (très négatif) à +1.0 (très positif), 0.0 = neutre
- themes : 1 à 3 valeurs parmi {THEMES}
- intention : 1 valeur parmi {INTENTIONS}

Règles :
- Emoji-only positif (🔥👏❤️) → sentiment positive 0.7, themes ['praise_product'], intention 'praise'
- Emoji-only neutre/inconnu (👀, 🤔, '.') → sentiment neutral 0.0, themes ['spam_or_noise'], intention 'other'
- Salutation religieuse seule ("barakAllahou fikoum", "qu'Allah te récompense") → positive 0.6, themes ['gratitude','religious_values'], intention 'gratitude'
- Demande de nouveau tome / suite → themes inclut 'request_new_title'
- Identification familiale ("ma fille adore", "mes enfants se reconnaissent") → themes inclut 'family_identification'
- Critique du prix → themes inclut 'complaint_price', sentiment negative
- Tag d'amis ("@untel regarde") → neutral 0.0, themes ['spam_or_noise'], intention 'other'

Sois strict sur le format. Ne sors RIEN hors de l'appel d'outil."""

TOOL = {
    'name': 'submit_analysis',
    'description': 'Stocke l\'analyse NLP du verbatim.',
    'input_schema': {
        'type': 'object',
        'properties': {
            'lang': {'type': 'string', 'enum': ['fr', 'ar', 'en', 'mixed', 'unknown']},
            'sentiment': {'type': 'string', 'enum': ['positive', 'neutral', 'negative']},
            'sentiment_score': {'type': 'number', 'minimum': -1.0, 'maximum': 1.0},
            'themes': {
                'type': 'array',
                'items': {'type': 'string', 'enum': THEMES},
                'minItems': 1,
                'maxItems': 3,
            },
            'intention': {'type': 'string', 'enum': INTENTIONS},
        },
        'required': ['lang', 'sentiment', 'sentiment_score', 'themes', 'intention'],
    },
}


# ---------- DB ----------

def fetch_pending(cur, source: str, limit: int | None) -> list[dict]:
    if source == 'contest':
        sql = """
            SELECT s.id, s.source, s.metadata->>'text' AS text
            FROM audience.signals s
            LEFT JOIN audience.verbatim_nlp n
                   ON n.entity_type='signal' AND n.entity_id=s.id
            WHERE s.signal_type='verbatim'
              AND s.metadata->>'text' IS NOT NULL
              AND length(s.metadata->>'text') > 0
              AND n.id IS NULL
            ORDER BY s.id
        """
    elif source == 'instagram':
        sql = """
            SELECT s.id, s.source, s.metadata->>'text' AS text
            FROM audience.signals s
            LEFT JOIN audience.verbatim_nlp n
                   ON n.entity_type='signal' AND n.entity_id=s.id
            WHERE s.signal_type='comment'
              AND s.source='instagram'
              AND s.metadata->>'text' IS NOT NULL
              AND length(s.metadata->>'text') > 0
              AND n.id IS NULL
            ORDER BY s.id
        """
    elif source == 'campaigns':
        sql = """
            SELECT c.id, 'mailerlite' AS source,
                   COALESCE(c.subject_clean, c.subject) AS text
            FROM clean.ml_campaigns c
            LEFT JOIN audience.verbatim_nlp n
                   ON n.entity_type='campaign' AND n.entity_id=c.id
            WHERE COALESCE(c.subject_clean, c.subject) IS NOT NULL
              AND length(COALESCE(c.subject_clean, c.subject)) > 0
              AND n.id IS NULL
            ORDER BY c.id
        """
    else:
        raise ValueError(f'Unknown source: {source}')

    if limit:
        sql += f' LIMIT {int(limit)}'

    cur.execute(sql)
    rows = []
    for row in cur.fetchall():
        rows.append({'entity_id': row[0], 'source': row[1], 'text': row[2]})
    return rows


def write_results(cur, results: list[dict], entity_type: str):
    if not results:
        return
    values = [
        (entity_type, r['entity_id'], r['source'], r['text'][:2000],
         r['lang'], r['sentiment'], r['sentiment_score'],
         r['themes'], r['intention'], MODEL)
        for r in results
    ]
    execute_values(cur, """
        INSERT INTO audience.verbatim_nlp
          (entity_type, entity_id, source, text, lang, sentiment,
           sentiment_score, themes, intention, model)
        VALUES %s
        ON CONFLICT (entity_type, entity_id) DO UPDATE SET
          lang = EXCLUDED.lang,
          sentiment = EXCLUDED.sentiment,
          sentiment_score = EXCLUDED.sentiment_score,
          themes = EXCLUDED.themes,
          intention = EXCLUDED.intention,
          model = EXCLUDED.model,
          processed_at = now()
    """, values)


# ---------- Anthropic ----------

async def classify_one(client: AsyncAnthropic, sem: asyncio.Semaphore,
                       item: dict) -> dict | None:
    text = (item['text'] or '').strip()[:1500]
    if not text:
        return None

    user_block = {'type': 'text', 'text': text}
    system = [{
        'type': 'text',
        'text': SYSTEM_PROMPT,
        'cache_control': {'type': 'ephemeral'},
    }]

    async with sem:
        for attempt in range(5):
            try:
                resp = await client.messages.create(
                    model=MODEL,
                    max_tokens=300,
                    system=system,
                    tools=[TOOL],
                    tool_choice={'type': 'tool', 'name': 'submit_analysis'},
                    messages=[{'role': 'user', 'content': [user_block]}],
                )
                for block in resp.content:
                    if block.type == 'tool_use' and block.name == 'submit_analysis':
                        out = dict(block.input)
                        out['entity_id'] = item['entity_id']
                        out['source'] = item['source']
                        out['text'] = text
                        return out
                return None
            except RateLimitError:
                await asyncio.sleep(2 ** attempt)
            except APIStatusError as e:
                if e.status_code >= 500:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print(f'  ✗ entity_id={item["entity_id"]} APIError {e.status_code}: {e.message}',
                          file=sys.stderr)
                    return None
    print(f'  ✗ entity_id={item["entity_id"]} max retries exceeded', file=sys.stderr)
    return None


async def run_batch(items: list[dict]) -> list[dict]:
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise RuntimeError('ANTHROPIC_API_KEY not set')
    client = AsyncAnthropic(api_key=api_key)
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [classify_one(client, sem, it) for it in items]
    return [r for r in await asyncio.gather(*tasks) if r]


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', required=True,
                    choices=['contest', 'instagram', 'campaigns'])
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--batch-size', type=int, default=200,
                    help='Commit après chaque batch (par défaut 200)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    entity_type = 'campaign' if args.source == 'campaigns' else 'signal'

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print(f'→ Fetching pending {args.source}...')
    pending = fetch_pending(cur, args.source, args.limit)
    print(f'  {len(pending)} items à traiter')

    if args.dry_run:
        for it in pending[:5]:
            print(' ', it['entity_id'], '|', it['text'][:120])
        return

    if not pending:
        print('Rien à faire.')
        return

    total_done = 0
    t0 = time.time()
    for i in range(0, len(pending), args.batch_size):
        batch = pending[i:i + args.batch_size]
        print(f'  batch {i // args.batch_size + 1} ({len(batch)} items)... ', end='', flush=True)
        results = asyncio.run(run_batch(batch))
        write_results(cur, results, entity_type)
        conn.commit()
        total_done += len(results)
        rate = total_done / max(time.time() - t0, 1)
        print(f'✓ {len(results)}/{len(batch)} ok ({rate:.1f}/s)')

    cur.execute("SELECT count(*) FROM audience.verbatim_nlp WHERE entity_type=%s",
                (entity_type,))
    total_db = cur.fetchone()[0]
    print(f'\n✓ Terminé : {total_done} traités, {total_db} lignes dans verbatim_nlp ({entity_type}).')


if __name__ == '__main__':
    main()
