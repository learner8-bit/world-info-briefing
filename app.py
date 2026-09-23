"""个人简报适配层。TrendRadar 负责采集、SQLite 存储、AI 客户端和通知发送。"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import html
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
import threading
import unicodedata
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo, reset_tzpath

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'vendor' / 'TrendRadar'))
os.environ.setdefault('LITELLM_LOCAL_MODEL_COST_MAP', 'True')
os.environ.setdefault('LITELLM_TELEMETRY', 'False')
import yaml
import pytz
# Windows没有系统IANA时区库，复用TrendRadar锁定的pytz数据。
reset_tzpath([str(Path(pytz.__file__).parent / 'zoneinfo')])
from dotenv import load_dotenv
from filelock import FileLock

OPERATION_LOCK = threading.Lock()


def utcnow():
    return datetime.now(timezone.utc)


def stamp(dt=None):
    return (dt or utcnow()).isoformat()


def parse_date(value, tz='UTC'):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return dt.replace(tzinfo=ZoneInfo(tz)) if dt.tzinfo is None else dt
    except ValueError:
        return None


def read_json(path, default=None):
    # 状态损坏必须报错，不能当成全新状态再次群发。
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8', newline='\n') as f:
        json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def atomic_yaml(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8', newline='\n') as f:
        yaml.safe_dump(value, f, allow_unicode=True, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def deep_merge(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            merged[key] = deep_merge(merged.get(key), value) if key in merged else value
        return merged
    return overlay


def configuration():
    load_dotenv(ROOT / '.env', override=False, interpolate=False)
    cfg = yaml.safe_load((ROOT / 'config/settings.yaml').read_text(encoding='utf-8'))
    sources = yaml.safe_load((ROOT / 'config/sources.yaml').read_text(encoding='utf-8'))
    user_settings = ROOT / 'config/user-settings.yaml'
    user_sources = ROOT / 'config/user-sources.yaml'
    if user_settings.exists():
        cfg = deep_merge(cfg, yaml.safe_load(user_settings.read_text(encoding='utf-8')) or {})
    if user_sources.exists():
        sources = deep_merge(sources, yaml.safe_load(user_sources.read_text(encoding='utf-8')) or {})
    # Docker needs to listen inside the container while the Compose port binding
    # still limits access to the Windows host. Native runs keep the YAML default.
    cfg['dashboard']['host'] = os.environ.get('DASHBOARD_HOST', cfg['dashboard']['host'])
    dashboard_port = os.environ.get('DASHBOARD_PORT')
    if dashboard_port:
        cfg['dashboard']['port'] = int(dashboard_port)
    validate(cfg, sources)
    return cfg, sources


def validate(cfg, sources):
    ZoneInfo(cfg['timezone'])
    b, s = cfg['briefing'], cfg['schedule']
    for name in ('max_items', 'max_daily_items', 'summary_chars', 'candidate_limit',
                 'per_source_limit', 'lookback_hours', 'dedup_days', 'max_input_chars', 'max_response_chars'):
        if type(b[name]) is not int or b[name] <= 0:
            raise ValueError(f'briefing.{name} 必须是正整数')
    mix = b.get('mix')
    if not isinstance(mix, dict) or set(mix) != {'politics_economy', 'technology', 'bioscience'}:
        raise ValueError('briefing.mix must define politics_economy, technology and bioscience')
    for key, rule in mix.items():
        if not isinstance(rule, dict) or not isinstance(rule.get('label'), str) or not rule['label'].strip():
            raise ValueError(f'briefing.mix.{key}.label is invalid')
        if type(rule.get('target')) is not int or rule['target'] < 0:
            raise ValueError(f'briefing.mix.{key}.target must be a non-negative integer')
        if not isinstance(rule.get('keywords'), list) or any(
            not isinstance(x, str) or not x.strip() for x in rule['keywords']
        ):
            raise ValueError(f'briefing.mix.{key}.keywords is invalid')
    if sum(rule['target'] for rule in mix.values()) > b['max_items']:
        raise ValueError('briefing.mix targets cannot exceed briefing.max_items')
    if b['max_items'] > b['max_daily_items'] or b['max_items'] > 50:
        raise ValueError('max_items 必须不超过每日上限，且单份最多50条')
    if not 0 <= b['min_score'] <= 1 or not b['categories'] or len(set(b['categories'])) != len(b['categories']):
        raise ValueError('筛选阈值或分类无效')
    if not 1 <= s['collect_minutes'] <= 1440 or not 1 <= s['catchup_minutes'] <= 1440:
        raise ValueError('采集间隔或补发窗口无效')
    if not s['times'] or len(set(s['times'])) != len(s['times']):
        raise ValueError('推送时间不能为空或重复')
    for t in s['times']:
        if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', t):
            raise ValueError('时间必须为 HH:MM')
    if not cfg['channels'] or not set(cfg['channels']) <= {'feishu', 'telegram', 'wework', 'email'}:
        raise ValueError('渠道支持 feishu / telegram / wework / email')
    if len(set(cfg['channels'])) != len(cfg['channels']):
        raise ValueError('渠道重复')
    retention = cfg['storage']['retention_days']
    if retention < max(b['dedup_days'], math.ceil(b['lookback_hours']/24), 2):
        raise ValueError('保留天数必须覆盖去重与候选窗口')
    prompt_path = (ROOT / 'config' / b['prompt']).resolve()
    if not prompt_path.is_relative_to((ROOT / 'config').resolve()) or not prompt_path.is_file():
        raise ValueError('Prompt 必须为 config 内存在的文件')
    ids = []
    for kind in ('rss', 'platforms'):
        for source in sources[kind]:
            ids.append(source['id'])
            if source.get('group', 'technology') not in mix:
                raise ValueError(f"source {source['id']} has an invalid group")
            if kind == 'rss' and not canonical_url(source['url']):
                raise ValueError('RSS URL无效')
            if kind == 'rss' and source.get('max_age_days', 3) <= 0:
                raise ValueError('RSS max_age_days 必须大于0')
    if len(set(ids)) != len(ids) or not ids:
        raise ValueError('来源ID必须唯一且至少配置一个来源')
    if not any(x.get('enabled', True) for k in ('rss', 'platforms') for x in sources[k]):
        raise ValueError('至少启用一个来源')
    if type(cfg['ai'].get('enabled')) is not bool or type(cfg['delivery'].get('enabled')) is not bool:
        raise ValueError('AI 与推送开关必须为布尔值')
    if not os.environ.get('AI_MODEL', cfg['ai']['model']).startswith('openai/'):
        raise ValueError('本适配层要求 AI_MODEL 使用 openai/模型名')
    if not canonical_url(os.environ.get('AI_API_BASE', cfg['ai']['api_base'])):
        raise ValueError('AI_API_BASE 无效')
    dashboard = cfg['dashboard']
    allowed_dashboard_hosts = {'127.0.0.1', 'localhost'}
    if os.environ.get('RUNNING_IN_DOCKER') == 'true':
        allowed_dashboard_hosts.add('0.0.0.0')
    if dashboard['host'] not in allowed_dashboard_hosts:
        raise ValueError('可视化界面仅允许监听本机地址')
    if type(dashboard['port']) is not int or not 1024 <= dashboard['port'] <= 65535:
        raise ValueError('dashboard.port 必须在 1024 到 65535 之间')


def editable_snapshot(cfg, sources):
    return {
        'settings': {
            'timezone': cfg['timezone'],
            'schedule': cfg['schedule'],
            'briefing': {
                key: cfg['briefing'][key] for key in (
                    'title', 'max_items', 'max_daily_items', 'min_score', 'summary_chars',
                    'candidate_limit', 'per_source_limit', 'lookback_hours', 'dedup_days',
                    'categories', 'mix'
                )
            },
            'ai': {'enabled': cfg['ai']['enabled']},
            'delivery': {'enabled': cfg['delivery']['enabled']},
            'channels': cfg['channels'],
            'storage': {'retention_days': cfg['storage']['retention_days']},
        },
        'sources': {'rss': sources['rss'], 'platforms': sources['platforms']},
    }


def save_editable_configuration(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get('settings'), dict):
        raise ValueError('设置数据格式无效')
    current_cfg, current_sources = configuration()
    settings = payload['settings']
    allowed = editable_snapshot(current_cfg, current_sources)['settings']
    if set(settings) != set(allowed):
        raise ValueError('设置字段不完整或包含未知字段')
    briefing_keys = set(allowed['briefing'])
    if set(settings.get('briefing', {})) != briefing_keys:
        raise ValueError('简报设置字段不完整')
    if set(settings.get('schedule', {})) != set(allowed['schedule']):
        raise ValueError('定时设置字段不完整')
    if set(settings.get('ai', {})) != {'enabled'} or set(settings.get('delivery', {})) != {'enabled'}:
        raise ValueError('功能开关字段无效')
    if set(settings.get('storage', {})) != {'retention_days'}:
        raise ValueError('存储设置字段无效')
    source_data = payload.get('sources')
    if not isinstance(source_data, dict) or set(source_data) != {'rss', 'platforms'}:
        raise ValueError('信息源数据格式无效')
    if not all(isinstance(source_data[k], list) for k in ('rss', 'platforms')):
        raise ValueError('信息源列表无效')
    overlay = {
        'timezone': settings['timezone'],
        'schedule': settings['schedule'],
        'briefing': settings['briefing'],
        'ai': settings['ai'],
        'delivery': settings['delivery'],
        'channels': settings['channels'],
        'storage': settings['storage'],
    }
    source_overlay = {'rss': source_data['rss'], 'platforms': source_data['platforms']}
    base_cfg = yaml.safe_load((ROOT / 'config/settings.yaml').read_text(encoding='utf-8'))
    base_sources = yaml.safe_load((ROOT / 'config/sources.yaml').read_text(encoding='utf-8'))
    validate(deep_merge(base_cfg, overlay), deep_merge(base_sources, source_overlay))
    settings_path = ROOT / 'config/user-settings.yaml'
    sources_path = ROOT / 'config/user-sources.yaml'
    backup_dir = ROOT / 'config/backups'
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_stamp = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    for path in (settings_path, sources_path):
        if path.exists():
            shutil.copy2(path, backup_dir / f'{path.stem}-{backup_stamp}{path.suffix}')
    atomic_yaml(settings_path, overlay)
    atomic_yaml(sources_path, source_overlay)
    return configuration()


def output_dir(cfg):
    return ROOT / cfg['storage']['output_dir']


def load_state(out):
    return read_json(out / 'state.json', {'version': 1, 'runs': {}, 'sent': {}})


def canonical_url(url):
    try:
        p = urlsplit(str(url).strip())
        if p.scheme not in ('https', 'http') or not p.hostname or p.username or p.password:
            return ''
        if any(c in str(url) for c in '\r\n\t <>'):
            return ''
        pairs = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                 if not k.lower().startswith('utm_') and k.lower() not in ('fbclid', 'gclid')]
        return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or '/', urlencode(pairs), ''))
    except ValueError:
        return ''


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()[:24]


def clean_text(value, limit):
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', '', str(value)))).strip()[:limit]


@contextlib.contextmanager
def quiet_upstream():
    # 上游异常日志可能带 API URL 或模型响应。统一不回显，状态仅记录类型与源ID。
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        yield


def collect(cfg, sources, out):
    from trendradar.crawler import DataFetcher
    from trendradar.crawler.rss.fetcher import RSSFetcher, RSSFeedConfig
    from trendradar.storage import LocalStorageBackend, convert_crawl_results_to_news_data
    now = datetime.now(ZoneInfo(cfg['timezone']))
    store = LocalStorageBackend(str(out / 'raw'), enable_txt=False, enable_html=False, timezone=cfg['timezone'])
    report = {'at': stamp(now), 'sources': {}, 'total_items': 0}
    net = sources['network']
    try:
        feeds = [s for s in sources['rss'] if s.get('enabled', True)]
        if feeds:
            fetcher = RSSFetcher([RSSFeedConfig(s['id'], s['name'], s['url'], max_items=100) for s in feeds],
                                 request_interval=net['request_interval_ms'], timeout=net['rss_timeout'],
                                 timezone=cfg['timezone'])
            with quiet_upstream():
                data = fetcher.fetch_all()
                if not store.save_rss_data(data):
                    raise RuntimeError('RSS storage failure')
            fetcher.session.close()
            for s in feeds:
                count = len(data.items.get(s['id'], []))
                report['sources'][s['id']] = {'ok': s['id'] not in data.failed_ids and count > 0, 'count': count}
                report['total_items'] += count
        platforms = [s for s in sources['platforms'] if s.get('enabled', True)]
        if platforms:
            with quiet_upstream():
                results, names, failed = DataFetcher(api_url=net.get('newsnow_api') or None).crawl_websites(
                    [(s['id'], s['name']) for s in platforms], request_interval=net['request_interval_ms'],
                    domain_rules={s['id']: s.get('expected_domain', '') for s in platforms})
                data = convert_crawl_results_to_news_data(results, names, failed, now.strftime('%H:%M'), now.strftime('%Y-%m-%d'))
                if not store.save_news_data(data):
                    raise RuntimeError('News storage failure')
            for s in platforms:
                count = len(results.get(s['id'], {}))
                report['sources'][s['id']] = {'ok': s['id'] not in failed and count > 0, 'count': count}
                report['total_items'] += count
        report['healthy'] = sum(v['ok'] for v in report['sources'].values())
        atomic_json(out / 'collection.json', report)
        with quiet_upstream():
            store.cleanup_old_data(cfg['storage']['retention_days'])
    finally:
        store.cleanup()
    print(f"采集：{report['healthy']}/{len(report['sources'])} 个源返回非空内容，{report['total_items']} 条；详情见 collection.json")
    if not report['healthy']:
        raise RuntimeError('全部来源失败或为空，已停止生成简报')
    return report


def _normalized_story_text(value):
    """Normalize punctuation, width and case while retaining Chinese and Latin text."""
    value = unicodedata.normalize('NFKC', html.unescape(str(value or ''))).lower()
    return re.sub(r'[\W_]+', '', value, flags=re.UNICODE)


def _text_ngrams(value, size=2):
    return {value[i:i + size] for i in range(max(0, len(value) - size + 1))}


def near_duplicate_text(left, right, *, summary=False):
    """Conservative same-language similarity check used while AI is disabled."""
    left = _normalized_story_text(left)
    right = _normalized_story_text(right)
    minimum = 30 if summary else 12
    if not left or not right:
        return False
    if left == right:
        return True
    if min(len(left), len(right)) < minimum:
        return False
    if min(len(left), len(right)) / max(len(left), len(right)) < 0.68:
        return False
    sequence = SequenceMatcher(None, left, right, autojunk=False).ratio()
    left_grams, right_grams = _text_ngrams(left), _text_ngrams(right)
    jaccard = len(left_grams & right_grams) / max(1, len(left_grams | right_grams))
    if summary:
        return sequence >= 0.93 or (sequence >= 0.86 and jaccard >= 0.78)
    return sequence >= 0.88 or (sequence >= 0.80 and jaccard >= 0.72)


def deduplicate_rows(rows, recent_titles=()):
    """Remove same-URL, same-title and conservative near-duplicate stories."""
    kept = []
    recent = [title for title in recent_titles if title]
    seen_urls = set()
    for row in rows:
        if row['url'] in seen_urls:
            continue
        if any(near_duplicate_text(row['title'], title) for title in recent):
            continue
        duplicate = False
        for previous in kept:
            if near_duplicate_text(row['title'], previous['title']):
                duplicate = True
                break
            if (row.get('summary') and previous.get('summary') and
                    near_duplicate_text(row['summary'], previous['summary'], summary=True)):
                duplicate = True
                break
        if duplicate:
            continue
        kept.append(row)
        seen_urls.add(row['url'])
    return kept


def candidates(cfg, sources, out, state, now=None):
    now = now or utcnow()
    b = cfg['briefing']
    source_map = {s['id']: s for k in ('rss', 'platforms') for s in sources[k] if s.get('enabled', True)}
    sent_urls = {url for url, info in state['sent'].items()
                 if parse_date(info['at']) > now - timedelta(days=b['dedup_days'])}
    grouped = {}
    for kind, table, id_col, name_table in [('rss', 'rss_items', 'feed_id', 'rss_feeds'),
                                          ('news', 'news_items', 'platform_id', 'platforms')]:
        for path in sorted((out / 'raw' / kind).glob('*.db'), reverse=True):
            day = parse_date(path.stem, cfg['timezone'])
            if not day or day + timedelta(days=1) < now - timedelta(hours=b['lookback_hours']):
                continue
            con = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
            con.row_factory = sqlite3.Row
            try:
                rows = con.execute(f'SELECT n.*, p.name AS source_name FROM {table} n JOIN {name_table} p ON n.{id_col}=p.id').fetchall()
            finally:
                con.close()
            for row in rows:
                source = source_map.get(row[id_col])
                if not source:
                    continue
                seen = parse_date(f"{path.stem}T{row['last_crawl_time']}", cfg['timezone'])
                if not seen or seen < now - timedelta(hours=b['lookback_hours']):
                    continue
                published = parse_date(row['published_at']) if kind == 'rss' else None
                if kind == 'rss':
                    if not published and not source.get('allow_undated', False):
                        continue
                    if published and (published < now - timedelta(days=source.get('max_age_days', 3)) or published > now + timedelta(hours=1)):
                        continue
                url = canonical_url(row['url'])
                if not url or url in sent_urls or url in grouped:
                    continue
                summary = clean_text(row['summary'] or '', b['max_input_chars']) if kind == 'rss' else ''
                grouped[url] = {'id': digest(url), 'url': url, 'title': clean_text(row['title'], 250),
                                'source_id': source['id'], 'source': row['source_name'],
                                'published_at': stamp(published) if published else '未知', 'seen_at': stamp(seen),
                                'summary': summary, 'evidence': 'RSS摘要' if summary else '仅标题信息',
                                'priority': source.get('priority', 2),
                                'group': source.get('group', 'technology'),
                                'role': source.get('role', source['id']),
                                'tier': source.get('tier', 'supplementary')}
    # 先按来源轮询，再做同 URL、同语种近似标题和近似摘要去重。
    # 跨语言同事件仍需 AI 才能可靠合并。
    queues = {}
    for item in sorted(grouped.values(), key=lambda r: r['published_at'] if r['published_at'] != '未知' else r['seen_at'], reverse=True):
        queues.setdefault(item['source_id'], []).append(item)
    order = sorted(queues, key=lambda sid: (source_map[sid].get('priority', 2), sid))
    selected = []
    for i in range(b['per_source_limit']):
        for sid in order:
            if i < len(queues[sid]):
                selected.append(queues[sid][i])
    recent_titles = [
        info.get('title', '') for info in state.get('sent', {}).values()
        if isinstance(info, dict) and parse_date(info.get('at')) and
        parse_date(info['at']) > now - timedelta(days=b['dedup_days'])
    ]
    return deduplicate_rows(selected, recent_titles)[:b['candidate_limit']]


def check_response(raw, rows, cfg, limit):
    b = cfg['briefing']
    if len(raw) > b['max_response_chars']:
        raise ValueError('模型输出过长')
    raw = raw.strip()
    if raw.startswith('```') and raw.endswith('```'):
        raw = re.sub(r'^```(?:json)?\s*', '', raw)[:-3].strip()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get('items'), list):
        raise ValueError('模型必须返回 items 数组')
    by_id = {r['id']: r for r in rows}
    events = []
    used = set()
    for x in payload['items']:
        if not isinstance(x, dict) or set(x) != {'title', 'category', 'score', 'summary', 'source_ids'}:
            raise ValueError('模型字段不符合约定')
        score = x['score']
        if type(score) not in (int, float) or not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError('模型评分无效')
        if x['category'] not in b['categories']:
            raise ValueError('模型分类不在配置中')
        ids = x['source_ids']
        if not isinstance(ids, list) or not 1 <= len(ids) <= 3 or any(not isinstance(i, str) or i not in by_id for i in ids) or len(set(ids)) != len(ids):
            raise ValueError('模型引用了无效来源ID')
        if any(i in used for i in ids):
            raise ValueError('同一来源条目被重复编入不同事件')
        used.update(ids)
        if any(not isinstance(x[k], str) or not x[k].strip() for k in ('title', 'summary')):
            raise ValueError('标题/摘要不能为空')
        if re.search(r'https?://|www\.', x['title'] + x['summary']):
            raise ValueError('不接受模型自行生成的链接')
        if score >= b['min_score']:
            events.append({'title': clean_text(x['title'], 80), 'category': x['category'], 'score': score,
                           'summary': clean_text(x['summary'], b['summary_chars']),
                           'sources': [by_id[i] for i in ids]})
    # 硬上限由代码执行，不依赖模型是否遵循Prompt。
    return sorted(events, key=lambda e: e['score'], reverse=True)[:limit]


def analyze(rows, cfg, state, limit):
    from trendradar.ai.client import AIClient
    if not cfg['ai']['enabled']:
        raise ValueError('AI 当前未启用，请先在可视化设置中开启')
    if not os.environ.get('AI_API_KEY'):
        raise ValueError('请先在 .env 填写 AI_API_KEY')
    ai = {k.upper(): v for k, v in cfg['ai'].items()}
    ai.update(API_KEY=os.environ['AI_API_KEY'], MODEL=os.environ.get('AI_MODEL', ai['MODEL']),
              API_BASE=os.environ.get('AI_API_BASE', ai['API_BASE']))
    cutoff = utcnow() - timedelta(days=cfg['briefing']['dedup_days'])
    recent = list(dict.fromkeys(v['title'] for v in state['sent'].values() if parse_date(v['at']) > cutoff))[-200:]
    prompt = (ROOT / 'config' / cfg['briefing']['prompt']).read_text(encoding='utf-8')
    request = {'categories': cfg['briefing']['categories'], 'max_items': limit,
               'summary_chars': cfg['briefing']['summary_chars'], 'min_score': cfg['briefing']['min_score'],
               'recent_events': recent, 'articles': rows}
    with quiet_upstream():
        raw = AIClient(ai).chat([{'role': 'system', 'content': prompt},
                                {'role': 'user', 'content': json.dumps(request, ensure_ascii=False)}],
                               **cfg['ai'].get('extra_params', {}))
    return check_response(raw, rows, cfg, limit)


def _keyword_match(text, keyword):
    keyword = keyword.lower()
    if keyword.isascii() and len(keyword) <= 3:
        return re.search(rf'(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])', text) is not None
    return keyword in text


def classify_without_ai(row, cfg):
    """Classify by configurable title/summary hints, then source group."""
    mix = cfg['briefing']['mix']
    default = row.get('group', 'technology')
    text = f"{row.get('title', '')} {row.get('summary', '')}".lower()
    scores = {
        key: sum(1 for keyword in rule['keywords'] if _keyword_match(text, keyword))
        for key, rule in mix.items()
    }
    best = max(scores.values(), default=0)
    if best == 0:
        return default
    winners = [key for key, score in scores.items() if score == best]
    return default if default in winners else winners[0]


def select_without_ai(rows, cfg, limit):
    """Apply soft targets with story, source and source-role diversity."""
    mix = cfg['briefing']['mix']
    classified = [(row, classify_without_ai(row, cfg)) for row in deduplicate_rows(rows)]
    chosen = []
    used = set()
    counts = {group: 0 for group in mix}
    source_counts = {}
    role_counts = {}

    def take(row, group):
        chosen.append((row, group))
        used.add(row['url'])
        counts[group] += 1
        source_counts[row['source_id']] = source_counts.get(row['source_id'], 0) + 1
        role = row.get('role', row['source_id'])
        role_counts[(group, role)] = role_counts.get((group, role), 0) + 1

    for group, rule in mix.items():
        # First take different source roles, then different sources, and only
        # use a second story from one source when a category would be short.
        for unique_role, source_cap in ((True, 1), (False, 1), (False, 2)):
            for row, row_group in classified:
                if counts[group] >= rule['target'] or len(chosen) >= limit:
                    break
                role = row.get('role', row['source_id'])
                if (row_group != group or row['url'] in used or
                        source_counts.get(row['source_id'], 0) >= source_cap or
                        (unique_role and role_counts.get((group, role), 0))):
                    continue
                take(row, group)

    for source_cap in (1, 2):
        for row, group in classified:
            if len(chosen) >= limit:
                break
            if row['url'] in used or source_counts.get(row['source_id'], 0) >= source_cap:
                continue
            take(row, group)

    events = []
    for row, group in chosen:
        events.append({
            'title': row['title'],
            'category': mix[group]['label'],
            'score': None,
            'summary': clean_text(
                row.get('summary') or 'Only a title is available; open the source to verify.',
                cfg['briefing']['summary_chars'],
            ),
            'sources': [row],
        })
    return events


def render(events, cfg, at, collection=None, label=''):
    lines = [f"{cfg['briefing']['title']} {label}".strip(), at, '']
    if collection:
        failed = [sid for sid, s in collection['sources'].items() if not s['ok']]
        lines.append(f"采集时间：{collection['at']}；可用来源 {collection['healthy']}/{len(collection['sources'])}")
        if failed:
            lines.append('本次来源缺失：' + '、'.join(failed))
    if not events:
        lines.append('本期没有达到筛选标准的新内容，不补凑条目。')
    if not cfg['ai']['enabled'] and events:
        lines += ['', '【原文速览｜未经过 AI 筛选】']
        for rule in cfg['briefing']['mix'].values():
            group = [event for event in events if event['category'] == rule['label']]
            if not group:
                continue
            lines += ['', f"【{rule['label']}｜{len(group)}条】"]
            for event in group:
                source = event['sources'][0]
                lines += [
                    event['title'],
                    event['summary'],
                    f"来源：{source['source']}｜{source['evidence']}｜发布：{source['published_at']}",
                    source['url'],
                    '',
                ]
        lines += [
            '按代表性来源、时间窗口、来源轮询、URL/近似标题/摘要去重和4/4/2软配额选取；'
            '没有事实核查、跨来源合并或AI质量评分。'
        ]
        return '\n'.join(lines)
        lines += ['', '【原文速览｜未经过 AI 筛选】']
        for e in events:
            source = e['sources'][0]
            lines += [e['title'], e['summary'],
                      f"来源：{source['source']}｜{source['evidence']}｜发布：{source['published_at']}",
                      source['url'], '']
        lines += ['仅按已启用来源、时间窗口和 URL 去重选取；没有事实核查、跨来源合并或 AI 质量评分。']
        return '\n'.join(lines)
    for cat in cfg['briefing']['categories']:
        group = [e for e in events if e['category'] == cat]
        if not group:
            continue
        lines += ['', f'【{cat}】']
        for e in group:
            lines += [e['title'], e['summary']]
            for s in e['sources']:
                lines += [f"来源：{s['source']}｜{s['evidence']}｜发布：{s['published_at']}", s['url']]
            lines.append('')
    lines += ['依据标题/RSS摘要整理，未抓取全文。研究细节、争议主张请核对原始链接。']
    return '\n'.join(lines)


def split_text(text, max_bytes):
    # 先按行、再按字符拆，确保中文和超长单行也不超限。
    chunks, current = [], ''
    for char in text:
        if len((current + char).encode('utf-8')) > max_bytes:
            chunks.append(current)
            current = ''
        current += char
    if current:
        chunks.append(current)
    return chunks


def send(channel, text, cfg, out, label):
    from trendradar.notification import senders
    def splitter(_data, target, *_args, max_bytes=3000, **_kwargs):
        # Telegram上游固定HTML格式；转义文本后再按字节拆，避免切开实体。
        safe_limit = max(100, max_bytes // 6) if target == 'telegram' else max_bytes
        parts = split_text(text, safe_limit)
        return [html.escape(p) for p in parts] if target == 'telegram' else parts
    common = dict(report_data={}, report_type=label, split_content_func=splitter)
    with quiet_upstream():
        if channel == 'feishu':
            return senders.send_to_feishu(os.environ['FEISHU_WEBHOOK_URL'], **common)
        if channel == 'wework':
            return senders.send_to_wework(os.environ['WEWORK_WEBHOOK_URL'], **common)
        if channel == 'telegram':
            return senders.send_to_telegram(os.environ['TELEGRAM_BOT_TOKEN'], os.environ['TELEGRAM_CHAT_ID'], **common)
        if channel == 'email':
            path = out / 'mail.html'
            path.write_text('<!doctype html><meta charset="utf-8"><pre style="white-space:pre-wrap">' + html.escape(text) + '</pre>', encoding='utf-8')
            return senders.send_to_email(os.environ['EMAIL_FROM'], os.environ['EMAIL_PASSWORD'], os.environ['EMAIL_TO'],
                                         label, str(path), custom_smtp_server=os.environ.get('EMAIL_SMTP_SERVER') or None,
                                         custom_smtp_port=int(os.environ['EMAIL_SMTP_PORT']) if os.environ.get('EMAIL_SMTP_PORT') else None,
                                         get_time_func=lambda: datetime.now(ZoneInfo(cfg['timezone'])))
    raise ValueError('未知渠道')


def require_secrets(cfg, ai=False, notification=False):
    needed = ['AI_API_KEY'] if ai else []
    fields = {'feishu': ['FEISHU_WEBHOOK_URL'], 'wework': ['WEWORK_WEBHOOK_URL'],
              'telegram': ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'],
              'email': ['EMAIL_FROM', 'EMAIL_PASSWORD', 'EMAIL_TO']}
    if notification:
        for channel in cfg['channels']:
            needed += fields[channel]
    missing = [name for name in needed if not os.environ.get(name)]
    if missing:
        raise ValueError('缺少环境变量：' + ', '.join(missing))
    for key, hosts in [('FEISHU_WEBHOOK_URL', ('open.feishu.cn', 'open.larksuite.com', 'www.feishu.cn')),
                       ('WEWORK_WEBHOOK_URL', ('qyapi.weixin.qq.com',))]:
        channel = 'feishu' if key.startswith('FEISHU') else 'wework'
        if notification and channel in cfg['channels']:
            value = os.environ[key]
            if not canonical_url(value) or urlsplit(value).scheme != 'https' or urlsplit(value).hostname not in hosts:
                raise ValueError(f'{key} 必须使用官方HTTPS地址')


def due_slot(cfg, state, now):
    slots = []
    # 跨午夜开机也可补上一个时段；只补最近一个，不连发多份。
    for offset in (1, 0):
        date = (now - timedelta(days=offset)).strftime('%Y-%m-%d')
        for t in cfg['schedule']['times']:
            dt = datetime.fromisoformat(date + 'T' + t).replace(tzinfo=ZoneInfo(cfg['timezone']))
            if 0 <= (now - dt).total_seconds() <= cfg['schedule']['catchup_minutes'] * 60:
                slots.append((dt, date + '_' + t.replace(':', '-')))
    if not slots:
        return None
    key = max(slots)[1]
    return None if state['runs'].get(key, {}).get('status') in ('complete', 'expired', 'quota') else key


def quota_limit(cfg, state, day):
    used = sum(len(v['events']) for v in state['runs'].values()
               if v['day'] == day and v['status'] in ('pending', 'complete'))
    return max(0, min(cfg['briefing']['max_items'], cfg['briefing']['max_daily_items'] - used))


def deliver(cfg, out, state, key, sender=send):
    run = state['runs'][key]
    for channel in run['channels']:
        if channel in run['delivered']:
            continue
        if not sender(channel, run['text'], cfg, out, cfg['briefing']['title']):
            raise RuntimeError(f'{channel} 推送未确认成功；下次仅重试未确认的渠道')
        run['delivered'].append(channel)
        atomic_json(out / 'state.json', state)
    run['status'] = 'complete'
    run['completed_at'] = stamp()
    for e in run['events']:
        for s in e['sources']:
            state['sent'][s['url']] = {'at': stamp(), 'title': e['title']}
    atomic_json(out / 'state.json', state)


def run_brief(cfg, sources, out, key, preview=False):
    state = load_state(out)
    now = datetime.now(ZoneInfo(cfg['timezone']))
    if not preview:
        # 已生成但失败的简报保持原文重试；超过24h过期，避免发送过时简报。
        for pending_key, run in state['runs'].items():
            if run['status'] == 'pending':
                if parse_date(run['created_at']) < utcnow() - timedelta(hours=24):
                    run['status'] = 'expired'
                    atomic_json(out / 'state.json', state)
                    continue
                deliver(cfg, out, state, pending_key)
                return
        if state['runs'].get(key, {}).get('status') in ('complete', 'quota', 'expired'):
            print('该时段已经处理，跳过重复推送')
            return
    day = now.strftime('%Y-%m-%d')
    limit = cfg['briefing']['max_items'] if preview else quota_limit(cfg, state, day)
    if limit == 0:
        state['runs'][key] = {'day': day, 'status': 'quota', 'events': [], 'created_at': stamp()}
        atomic_json(out / 'state.json', state)
        print('达到每日条数上限，跳过本期')
        return
    collection = read_json(out / 'collection.json')
    if not collection or not collection['healthy'] or parse_date(collection['at']) < utcnow() - timedelta(minutes=cfg['schedule']['collect_minutes'] * 2 + 10):
        raise RuntimeError('没有足够新的成功采集记录；请先 collect')
    rows = candidates(cfg, sources, out, state)
    # A source that failed in this collection can still have rows in a recent
    # local database. Do not present those cached rows as today's fresh fetch.
    healthy_source_ids = {sid for sid, result in collection['sources'].items() if result['ok']}
    rows = [row for row in rows if row['source_id'] in healthy_source_ids]
    if not rows:
        events = []
    else:
        events = analyze(rows, cfg, state, limit) if cfg['ai']['enabled'] else select_without_ai(rows, cfg, limit)
    text = render(events, cfg, now.isoformat(timespec='minutes'), collection, '预览' if preview else '')
    report = {'created_at': stamp(), 'day': day, 'status': 'pending', 'events': events,
              'candidate_count': len(rows), 'channels': list(cfg['channels']), 'delivered': [], 'text': text}
    path = out / 'briefings' / ('preview' if preview else key)
    atomic_json(path.with_suffix('.json'), report)
    path.with_suffix('.md').write_text(text, encoding='utf-8')
    if preview:
        print(f'已生成预览：{len(events)} 条，未推送、未写已读状态。见 output/briefings/preview.md')
        return
    state['runs'][key] = report
    atomic_json(out / 'state.json', state)  # 先落盘，再发送，失败可恢复
    deliver(cfg, out, state, key)
    print(f'简报完成：{len(events)} 条；{key}')


def prune(cfg, out):
    cutoff = utcnow() - timedelta(days=cfg['storage']['retention_days'])
    state = load_state(out)
    state['runs'] = {k: v for k, v in state['runs'].items() if parse_date(v['created_at']) > cutoff}
    state['sent'] = {k: v for k, v in state['sent'].items() if parse_date(v['at']) > cutoff}
    atomic_json(out / 'state.json', state)
    for path in (out / 'briefings').glob('*'):
        if path.is_file() and path.suffix in ('.json', '.md') and path.stat().st_mtime < cutoff.timestamp():
            path.unlink()


def tick(cfg, sources, out):
    state = load_state(out)
    now = datetime.now(ZoneInfo(cfg['timezone']))
    previous = read_json(out / 'collection.json')
    if not previous or parse_date(previous['at']) < utcnow() - timedelta(minutes=cfg['schedule']['collect_minutes']):
        collect_once(cfg, sources, out, wait=False)
    if cfg['delivery']['enabled']:
        pending = any(r['status'] == 'pending' for r in state['runs'].values())
        key = due_slot(cfg, state, now)
        if pending or key:
            require_secrets(cfg, ai=cfg['ai']['enabled'] and not pending, notification=True)
            run_brief(cfg, sources, out, key or now.strftime('%Y-%m-%d_%H-%M'))
    prune(cfg, out)


def collect_once(cfg, sources, out, wait=True):
    acquired = OPERATION_LOCK.acquire(blocking=wait)
    if not acquired:
        return None
    try:
        with FileLock(str(out / '.operation.lock'), timeout=0 if not wait else 300):
            return collect(cfg, sources, out)
    finally:
        OPERATION_LOCK.release()


def dashboard_payload():
    cfg, sources = configuration()
    out = output_dir(cfg)
    state = load_state(out)
    collection = read_json(out / 'collection.json', {})
    action = read_json(out / 'dashboard-action.json', {'status': 'idle'})
    try:
        recent = candidates(cfg, sources, out, state)[:24]
    except (OSError, sqlite3.Error, ValueError):
        recent = []
    secret_status = {
        'ai_api_key': bool(os.environ.get('AI_API_KEY')),
        'feishu_webhook': bool(os.environ.get('FEISHU_WEBHOOK_URL')),
    }
    return {
        **editable_snapshot(cfg, sources),
        'collection': collection,
        'worker': read_json(out / 'worker.json', {}),
        'action': action,
        'recent': recent,
        'secret_status': secret_status,
        'project_path': str(ROOT),
        'version': '1.1.0',
    }


def start_dashboard_collection():
    cfg, sources = configuration()
    out = output_dir(cfg)
    current = read_json(out / 'dashboard-action.json', {'status': 'idle'})
    started = parse_date(current.get('started_at'))
    if current.get('status') == 'running' and started and started > utcnow() - timedelta(hours=2):
        return False

    def worker():
        atomic_json(out / 'dashboard-action.json', {'status': 'running', 'started_at': stamp()})
        try:
            latest_cfg, latest_sources = configuration()
            result = collect_once(latest_cfg, latest_sources, output_dir(latest_cfg), wait=False)
            if result is None:
                atomic_json(out / 'dashboard-action.json', {'status': 'busy', 'finished_at': stamp()})
            else:
                atomic_json(out / 'dashboard-action.json', {
                    'status': 'complete', 'finished_at': stamp(), 'healthy': result['healthy'],
                    'total_items': result['total_items']
                })
        except Exception as exc:
            atomic_json(out / 'dashboard-action.json', {
                'status': 'error', 'finished_at': stamp(), 'error_type': type(exc).__name__
            })

    threading.Thread(target=worker, name='dashboard-collection', daemon=True).start()
    return True


def run_dashboard(cfg):
    from web_dashboard import serve_dashboard
    serve_dashboard(
        ROOT, cfg['dashboard']['host'], cfg['dashboard']['port'],
        dashboard_payload, save_editable_configuration, start_dashboard_collection,
    )


def main():
    parser = argparse.ArgumentParser(description='TrendRadar全球信息简报')
    parser.add_argument('action', choices=['validate', 'ready', 'collect', 'preview', 'run', 'dashboard', 'serve', 'status', 'health', 'test-notification'])
    args = parser.parse_args()
    cfg, sources = configuration()
    out = output_dir(cfg)
    out.mkdir(parents=True, exist_ok=True)
    if args.action == 'validate':
        print('配置校验通过；未访问网络、未调用模型、未发送通知。')
        print('AI：' + ('已启用' if cfg['ai']['enabled'] else '未启用'))
        print('定时：' + ', '.join(cfg['schedule']['times']) + '；时区：' + cfg['timezone'])
        print(f"可视化界面：http://{cfg['dashboard']['host']}:{cfg['dashboard']['port']}")
        return
    if args.action == 'ready':
        if cfg['delivery']['enabled']:
            require_secrets(cfg, ai=cfg['ai']['enabled'], notification=True)
        print('当前启用功能的必要配置已就绪；这不代表外部连接已验证。')
        return
    if args.action == 'status':
        state = load_state(out)
        print(json.dumps({'collection': read_json(out / 'collection.json'),
                          'worker': read_json(out / 'worker.json'),
                          'runs': {k: {'status': v['status'], 'count': len(v['events']), 'delivered': v.get('delivered', [])}
                                   for k, v in list(state['runs'].items())[-10:]}}, ensure_ascii=False, indent=2))
        return
    if args.action == 'health':
        status = read_json(out / 'worker.json', {})
        if status.get('status') == 'error':
            raise RuntimeError('最近一轮运行失败，请查看 status')
        if not status.get('at') or parse_date(status['at']) < utcnow() - timedelta(hours=1):
            raise RuntimeError('调度进程心跳超过1小时未更新')
        return
    if args.action == 'dashboard':
        with FileLock(str(out / '.dashboard-server.lock'), timeout=0):
            run_dashboard(cfg)
        return
    if args.action == 'serve':
        with FileLock(str(out / '.service.lock'), timeout=0), FileLock(str(out / '.dashboard-server.lock'), timeout=0):
            threading.Thread(target=run_dashboard, args=(cfg,), name='dashboard', daemon=True).start()
            print(f"常驻采集与可视化界面已启动：http://{cfg['dashboard']['host']}:{cfg['dashboard']['port']}", flush=True)
            while True:
                atomic_json(out / 'worker.json', {'at': stamp(), 'status': 'running'})
                delay = 60
                try:
                    cfg, sources = configuration()
                    if output_dir(cfg) != out:
                        raise ValueError('修改 output_dir 后必须重启')
                    tick(cfg, sources, out)
                    atomic_json(out / 'worker.json', {'at': stamp(), 'status': 'ok'})
                except Exception as exc:
                    atomic_json(out / 'worker.json', {'at': stamp(), 'status': 'error', 'error_type': type(exc).__name__})
                    print('运行失败：' + type(exc).__name__ + '；检查配置与网络。30分钟后重试。', flush=True)
                    delay = 1800
                time.sleep(delay)
        return
    # 手动操作用独立锁；可视化界面与常驻采集也遵循同一把锁。
    with FileLock(str(out / '.operation.lock'), timeout=0):
        if args.action == 'collect':
            collect(cfg, sources, out)
        elif args.action == 'preview':
            require_secrets(cfg, ai=cfg['ai']['enabled'])
            run_brief(cfg, sources, out, 'preview', preview=True)
        elif args.action == 'run':
            if not cfg['delivery']['enabled']:
                raise ValueError('请先启用推送')
            require_secrets(cfg, ai=cfg['ai']['enabled'], notification=True)
            collect(cfg, sources, out)
            now = datetime.now(ZoneInfo(cfg['timezone']))
            key = due_slot(cfg, load_state(out), now) or now.strftime('%Y-%m-%d_%H-%M')
            run_brief(cfg, sources, out, key)
        elif args.action == 'test-notification':
            require_secrets(cfg, notification=True)
            for channel in cfg['channels']:
                if not send(channel, cfg['briefing']['title'] + '\n这是一条连接测试消息，不是新闻简报。', cfg, out, '连接测试'):
                    raise RuntimeError(f'{channel} 连接测试失败')
            print('通知测试成功')


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    try:
        main()
    except KeyboardInterrupt:
        print('已停止')
    except Exception as exc:
        # 不打印第三方异常正文；手动validate等本地错误可见，无敏感值。
        msg = str(exc) if isinstance(exc, (ValueError, RuntimeError)) and type(exc).__module__ == 'builtins' else type(exc).__name__
        for key, value in os.environ.items():
            if value and any(tag in key for tag in ('KEY', 'TOKEN', 'PASSWORD', 'WEBHOOK')):
                msg = msg.replace(value, '[REDACTED]')
        print('失败：' + msg, file=sys.stderr)
        sys.exit(1)
