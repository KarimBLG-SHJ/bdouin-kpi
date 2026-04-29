#!/usr/bin/env python3
"""
scheduler.py — Railway worker process: orchestre tous les collecteurs.

Frequencies:
  Toutes les 6h  : reviews iOS+Android, MailerLite, IG comments, web mentions, gmail récent
  Quotidien      : PrestaShop orders, GA4, Search Console, ASC, Sofiadis B2B
  Hebdomadaire   : Drive sync (OAuth), Play Vitals, GA4 backfill complet
  Toutes les 30min : Reviews API last 7d (très léger)

Usage Railway:
    Procfile: worker: python3 scheduler.py
    Variables: BDOUIN_DB, GA4_CREDENTIALS_JSON, ASC_PRIVATE_KEY, OAUTH_CLIENT, DRIVE_TOKEN

Logs : stdout (visible dans Railway logs)
"""

import os
import sys
import time
import traceback
import subprocess
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Timezone Dubai (UAE) pour cohérence avec les autres agents BDouin
TZ = 'Asia/Dubai'
HERE = os.path.dirname(os.path.abspath(__file__))


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run_script(script_name, args=None):
    """Run a collector script as subprocess and log output."""
    args = args or []
    cmd = [sys.executable, os.path.join(HERE, script_name)] + args
    log(f"▶ {script_name} {' '.join(args)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            # Print last 5 lines of output to keep logs concise
            lines = (result.stdout or '').strip().split('\n')
            for line in lines[-5:]:
                log(f"  {line}")
            log(f"✓ {script_name} done")
        else:
            log(f"✗ {script_name} failed: {result.stderr[:500]}")
    except subprocess.TimeoutExpired:
        log(f"✗ {script_name} timed out")
    except Exception as e:
        log(f"✗ {script_name} exception: {e}")


def safe_run(fn_name, fn):
    def wrapper():
        log(f"=== {fn_name} START ===")
        try:
            fn()
        except Exception as e:
            log(f"✗ {fn_name} crashed: {e}")
            log(traceback.format_exc()[:800])
        log(f"=== {fn_name} END ===\n")
    return wrapper


# ─── Job functions ────────────────────────────────────────────────────────────

def job_presta():
    """PrestaShop incremental: orders + customers + carts."""
    for resource in ['orders', 'order_details', 'customers', 'addresses',
                     'carts', 'order_histories', 'order_payments', 'order_invoices',
                     'stock_movements']:
        run_script('collect_presta.py', ['--resource', resource])


def job_ga4_recent():
    """GA4 last 7 days only — light, frequent."""
    run_script('collect_ga4.py', ['--days', '7'])


def job_ga4_full():
    """GA4 full history — weekly backfill."""
    run_script('collect_ga4.py')


def job_gsc():
    """Google Search Console — daily."""
    run_script('collect_gsc.py')


def job_mailerlite():
    """MailerLite subscribers + groups."""
    run_script('collect_mailerlite.py', ['--resource', 'subscriber_groups'])


def job_meta():
    """Meta Instagram + Facebook."""
    run_script('collect_meta.py')


def job_mentions():
    """Reddit + YouTube web mentions."""
    run_script('collect_mentions.py')


def job_alerts_rss():
    """Google Alerts RSS feeds (when configured)."""
    run_script('collect_alerts.py')


def job_playstore_reviews():
    """Google Play reviews (last 7 days via official API)."""
    run_script('collect_playstore_api.py', ['--resource', 'reviews'])


def job_playstore_metrics():
    """Google Play vitals (crash, ANR, etc.)."""
    run_script('collect_playstore_api.py', ['--resource', 'metrics'])


def job_asc_reviews():
    """App Store Connect — apps + ratings."""
    run_script('collect_asc.py', ['--resource', 'apps'])
    run_script('collect_asc.py', ['--resource', 'ratings'])


def job_asc_revenue():
    """App Store Connect — financial reports (monthly cadence)."""
    run_script('collect_asc.py', ['--resource', 'revenue'])


def job_gmail():
    """Gmail incremental — last 7 days only."""
    pwd = os.environ.get('GMAIL_APP_PASSWORD', '')
    if pwd:
        run_script('collect_gmail.py', ['--password', pwd, '--days', '7'])
    else:
        log('  ⚠ GMAIL_APP_PASSWORD not set, skipping')


def job_drive():
    """Drive metadata sync via OAuth."""
    run_script('collect_drive_oauth.py')


def job_incremental_pipeline():
    """Refresh CLEAN + GOLD depuis les tables public.* — après les collectes quotidiennes."""
    run_script('pipeline_09_incremental.py')


# ─── Scheduler config ─────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("BDouin data collectors scheduler starting")
    log("=" * 60)

    sched = BlockingScheduler(
        timezone=TZ,
        executors={'default': ThreadPoolExecutor(2)},  # max 2 concurrent jobs
        job_defaults={'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 300},
    )

    # ── 6h cycle (4× par jour : 03h, 09h, 15h, 21h Dubai)
    six_hour = CronTrigger(hour='3,9,15,21', minute=0, timezone=TZ)
    sched.add_job(safe_run('mailerlite',         job_mailerlite),         six_hour, id='ml')
    sched.add_job(safe_run('meta',               job_meta),               six_hour, id='meta')
    sched.add_job(safe_run('mentions',           job_mentions),           six_hour, id='mentions')
    sched.add_job(safe_run('playstore_reviews',  job_playstore_reviews),  six_hour, id='ps_reviews')
    sched.add_job(safe_run('alerts_rss',         job_alerts_rss),         six_hour, id='alerts')

    # ── Quotidien (1h Dubai)
    daily = CronTrigger(hour=1, minute=0, timezone=TZ)
    sched.add_job(safe_run('presta',             job_presta),             daily, id='presta')
    sched.add_job(safe_run('ga4_recent',         job_ga4_recent),         daily, id='ga4_d')
    sched.add_job(safe_run('gsc',                job_gsc),                daily, id='gsc')
    sched.add_job(safe_run('asc_reviews',        job_asc_reviews),        daily, id='asc_r')
    sched.add_job(safe_run('gmail',              job_gmail),              daily, id='gmail')

    # ── Hebdomadaire (dimanche 2h Dubai)
    weekly = CronTrigger(day_of_week='sun', hour=2, minute=0, timezone=TZ)
    sched.add_job(safe_run('drive',              job_drive),              weekly, id='drive')
    sched.add_job(safe_run('playstore_metrics',  job_playstore_metrics),  weekly, id='ps_metrics')
    sched.add_job(safe_run('ga4_full',           job_ga4_full),           weekly, id='ga4_full')

    # ── Pipeline incrémental (4h Dubai — après les collectes daily 1h + cycle 6h 3h)
    sched.add_job(safe_run('incremental_pipeline', job_incremental_pipeline),
                  CronTrigger(hour=4, minute=0, timezone=TZ), id='incremental')

    # ── Mensuel (1er du mois 5h Dubai)
    monthly = CronTrigger(day=1, hour=5, minute=0, timezone=TZ)
    sched.add_job(safe_run('asc_revenue',        job_asc_revenue),        monthly, id='asc_rev')

    log(f"\n{len(sched.get_jobs())} jobs scheduled:")
    for j in sched.get_jobs():
        log(f"  {j.id:20s} → {j.trigger}")

    log("\nScheduler running (timezone: " + TZ + ")...\n")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log("Scheduler stopped")


if __name__ == '__main__':
    main()
