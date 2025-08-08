# -*- coding: utf-8 -*-
"""
企业微信「微信客服 ↔ 自建应用」统一回调程序（含自愈补拉）
- 回调 URL： http(s)://你的域名/hook_path
- 客服绑定：在客服里选择「通过自建应用管理此账号」绑定到本应用
- 支持消息：
    * text：普通文本
    * link：链接卡片（公众号、知乎等分享）
- 自愈：periodic_sync_loop() 每隔 SYNC_INTERVAL 秒按游标补拉，防止挂掉期间丢消息
"""
import os
from flask import Flask, request, jsonify
from xml.dom.minidom import parseString
import time, os, sys, json, threading, subprocess
import requests
from datetime import datetime

# ============== 加解密依赖 ==============
from WXBizMsgCrypt3 import WXBizMsgCrypt

app = Flask(__name__)

# ============== 必填配置（按实际修改） ==============
# 自建应用 回调验签参数（应用管理 → 你的应用 → 接收消息）
APP_TOKEN = os.environ.get("APP_TOKEN", "")
APP_ENCODING_AES_KEY = os.environ.get("APP_ENCODING_AES_KEY", "")
CORP_ID = os.environ.get("CORP_ID", "")
KF_SECRET = os.environ.get("KF_SECRET", "")
APP_SECRET = os.environ.get("APP_SECRET", "")

# 你的转发脚本（保持你自己的）
COMMAND_PY = "command.py"

# 运行参数
CURSOR_FILE = "kf_cursor.json"   # {OpenKfId: cursor}
SEEN_FILE   = "kf_seen.json"     # {OpenKfId: [msgid...]} 去重用
LOG_FILE    = "history.log"
CHANNEL_KF  = 9                  # 客服来源
CHANNEL_APP = 0                  # 应用来源（内部）
POLL_LIMIT  = 1000               # 每次拉取上限
SYNC_INTERVAL = 60               # 自愈补拉间隔（秒）
# ===================================================

wxcpt = WXBizMsgCrypt(APP_TOKEN, APP_ENCODING_AES_KEY, CORP_ID)
_kf_token_cache = {"token": None, "expire_at": 0}

def now_ts(): return int(time.time())

def append_log(line: str):
    with open(LOG_FILE, "a+", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} {line}\n")

def _gettoken_by_secret(secret: str):
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    r = requests.get(url, params={"corpid": CORP_ID, "corpsecret": secret}, timeout=10)
    r.raise_for_status()
    return r.json()

def get_kf_access_token():
    """
    优先 KF_SECRET；若缺失/失败则用 APP_SECRET。
    注意：若走 APP_SECRET，需在自建应用的 API 权限里勾选“微信客服”相关能力。
    """
    global _kf_token_cache
    if _kf_token_cache["token"] and _kf_token_cache["expire_at"] - now_ts() > 120:
        return _kf_token_cache["token"]

    first_err = None
    if KF_SECRET:
        try:
            data = _gettoken_by_secret(KF_SECRET)
            if data.get("errcode") == 0:
                _kf_token_cache["token"] = data["access_token"]
                _kf_token_cache["expire_at"] = now_ts() + 6600
                return _kf_token_cache["token"]
            first_err = data
            append_log(f"[WARN] gettoken by KF_SECRET failed: {data}")
        except Exception as e:
            first_err = {"exception": str(e)}
            append_log(f"[WARN] gettoken by KF_SECRET exception: {e}")

    if APP_SECRET:
        try:
            data = _gettoken_by_secret(APP_SECRET)
            if data.get("errcode") == 0:
                _kf_token_cache["token"] = data["access_token"]
                _kf_token_cache["expire_at"] = now_ts() + 6600
                append_log("[INFO] fallback to APP_SECRET for kf access_token")
                return _kf_token_cache["token"]
            append_log(f"[ERR] gettoken by APP_SECRET failed: {data}")
        except Exception as e:
            append_log(f"[ERR] gettoken by APP_SECRET exception: {e}")

    raise RuntimeError(f"gettoken failed. first_err={first_err}")

def load_cursors():
    if not os.path.exists(CURSOR_FILE): return {}
    try:
        with open(CURSOR_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cursors(cursors: dict):
    tmp = CURSOR_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cursors, f, ensure_ascii=False)
    os.replace(tmp, CURSOR_FILE)

def load_seen():
    if not os.path.exists(SEEN_FILE): return {}
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_seen(seen: dict):
    tmp = SEEN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False)
    os.replace(tmp, SEEN_FILE)

def log_and_forward(user_id: str, content: str, channel: int, msg_type: int):
    """msg_type: 0=文本（我们把 link 也整理成文本内容转发）"""
    append_log(f"[ch{channel}] {user_id}: {content[:200]}")
    try:
        subprocess.Popen(
            ["python3", COMMAND_PY, str(user_id), str(content), str(channel), str(msg_type)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        append_log(f"[ERR] start command.py failed: {e}")

def get_tag_text(doc, tag_name):
    nodes = doc.getElementsByTagName(tag_name)
    if not nodes or not nodes[0].childNodes:
        return ""
    return (nodes[0].childNodes[0].data or "").strip()

def kf_sync_msg_once(event_token: str, open_kfid: str):
    """
    拉取并处理客服消息（支持 text + link）：
    - with token → no token → cold start 三段重试
    - 显式带 open_kfid（避免 95000）
    - 基于 msgid 去重（SEEN_FILE）
    - 更新该 open_kfid 的游标（CURSOR_FILE）
    """
    cursors = load_cursors()
    cursor = cursors.get(open_kfid, "")

    # 去重集合（每个 open_kfid 一组，只保留最近 5000 条）
    seen = load_seen()
    seen_set = set(seen.get(open_kfid, []))

    # token
    try:
        access_token = get_kf_access_token()
    except Exception as e:
        append_log(f"[ERR] get_kf_access_token: {e}")
        return

    url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/sync_msg?access_token={access_token}"

    def _call(cursor_val: str, use_token: bool):
        body = {
            "cursor": cursor_val or "",
            "limit": POLL_LIMIT,
            "open_kfid": open_kfid
        }
        if use_token and event_token:
            body["token"] = event_token
        r = requests.post(url, json=body, timeout=10)
        r.raise_for_status()
        return r.json()

    # 三段式重试
    try:
        data = _call(cursor, use_token=True)
        if data.get("errcode") != 0:
            append_log(f"[WARN] sync with token failed: {data} -> fallback no-token")
            data = _call(cursor, use_token=False)
        if data.get("errcode") != 0:
            append_log(f"[WARN] sync no-token failed: {data} -> retry from scratch")
            data = _call("", use_token=False)
    except Exception as e:
        append_log(f"[ERR] kf/sync_msg request: {e}")
        return

    if data.get("errcode") != 0:
        append_log(f"[ERR] kf/sync_msg resp(final): {data}")
        return

    # 处理消息
    got_any = False
    msg_list = data.get("msg_list", []) or []
    for m in msg_list:
        # 幂等：msgid 去重
        msgid = str(m.get("msgid") or "")
        if msgid and msgid in seen_set:
            continue

        # 只接微信外部用户
        if m.get("origin") != 3:
            continue

        # 1) 文本
        if m.get("msgtype") == "text":
            got_any = True
            user = m.get("external_userid", "wx_external")
            text = (m.get("text") or {}).get("content", "").strip()
            if text:
                content = f"[{open_kfid}] {text}"
                log_and_forward(user, content, CHANNEL_KF, 0)
                if msgid: seen_set.add(msgid)

        # 2) 链接卡片（公众号/知乎等）
        elif m.get("msgtype") == "link":
            got_any = True
            user = m.get("external_userid", "wx_external")
            link = m.get("link") or {}
            title = (link.get("title") or "").strip()
            url_  = (link.get("url") or "").strip()
            desc  = (link.get("desc") or "").strip()
            if url_:
                content = f"[{open_kfid}] 🔗 {title or '链接'}\n{url_}\n{desc}"
                log_and_forward(user, content, CHANNEL_KF, 0)
                if msgid: seen_set.add(msgid)

        # 其他类型暂不处理（image/voice/file 等可后续扩展）

    # 更新游标
    next_cursor = data.get("next_cursor")
    if isinstance(next_cursor, str):
        cursors[open_kfid] = next_cursor
        save_cursors(cursors)

    # 持久化去重集合（限 5000）
    seen[open_kfid] = list(seen_set)[-5000:]
    try:
        save_seen(seen)
    except Exception as e:
        append_log(f"[WARN] save seen failed: {e}")

    if not got_any:
        append_log("[INFO] kf/sync_msg OK but no new text")

def handle_kf_event(doc):
    """
    处理客服事件（MsgType=event, Event=kf_msg_or_event）
    """
    event = get_tag_text(doc, "Event").lower()
    change_type = get_tag_text(doc, "ChangeType").lower()
    open_kfid = get_tag_text(doc, "OpenKfId")
    append_log(f"[DEBUG] kf event for OpenKfId={open_kfid}")

    if not ("kf" in event or "kf" in change_type):
        return False

    event_token = get_tag_text(doc, "Token")
    if not event_token:
        append_log("[WARN] kf event without Token")
        return True

    # 避免阻塞回调，异步拉取
    threading.Thread(
        target=kf_sync_msg_once, args=(event_token, open_kfid), daemon=True
    ).start()
    return True

# ============== 调用客服账号列表（自愈用） ==============
def list_kf_accounts():
    """
    返回当前 access_token 可管理的客服 open_kfid 列表
    """
    try:
        token = get_kf_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/account/list?access_token={token}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("errcode") != 0:
            append_log(f"[ERR] kf/account/list: {data}")
            return []
        accounts = [item.get("open_kfid") for item in (data.get("account_list") or []) if item.get("open_kfid")]
        return accounts
    except Exception as e:
        append_log(f"[ERR] list_kf_accounts exception: {e}")
        return []

# 简单的并发保护：避免同一 kfid 同时多次拉取（事件 + 定时器并发）
_sync_flags = {}
_sync_lock = threading.Lock()

def _run_sync_guarded(open_kfid: str, event_token: str = ""):
    with _sync_lock:
        if _sync_flags.get(open_kfid):
            return
        _sync_flags[open_kfid] = True
    try:
        kf_sync_msg_once(event_token, open_kfid)
    finally:
        with _sync_lock:
            _sync_flags[open_kfid] = False

def periodic_sync_loop():
    """
    后台循环：定时对所有客服账号做一次“无 token 的游标续拉”
    即使没有事件也能把漏的消息补回来
    """
    append_log(f"[INFO] periodic sync loop started, interval={SYNC_INTERVAL}s")
    while True:
        try:
            kfids = list_kf_accounts()
            if not kfids:
                append_log("[WARN] periodic sync: no kf accounts visible")
            for kfid in kfids:
                threading.Thread(target=_run_sync_guarded, args=(kfid, ""), daemon=True).start()
        except Exception as e:
            append_log(f"[ERR] periodic sync loop: {e}")
        time.sleep(SYNC_INTERVAL)

# ============== 回调入口 ==============
@app.route("/hook_path", methods=["GET", "POST"])
def hook_path():
    if request.method == "GET":
        msg_signature = request.args.get('msg_signature', '')
        timestamp    = request.args.get('timestamp', '')
        nonce        = request.args.get('nonce', '')
        echostr      = request.args.get('echostr', '')
        ret, sEchoStr = wxcpt.VerifyURL(msg_signature, timestamp, nonce, echostr)
        if ret != 0:
            append_log(f"[ERR] VerifyURL ret={ret}")
            return "failed"
        return sEchoStr

    msg_signature = request.args.get('msg_signature', '')
    timestamp    = request.args.get('timestamp', '')
    nonce        = request.args.get('nonce', '')
    data         = request.data.decode('utf-8')

    ret, sMsg = wxcpt.DecryptMsg(data, msg_signature, timestamp, nonce)
    if ret != 0:
        append_log(f"[ERR] DecryptMsg ret={ret}")
        return "failed"

    try:
        doc = parseString(sMsg)
    except Exception as e:
        append_log(f"[ERR] parse XML: {e}")
        return "success"

    # 额外日志，防跨企业/跨应用
    corp_in_msg  = get_tag_text(doc, "ToUserName")  # 企业ID
    agent_in_msg = get_tag_text(doc, "AgentID")     # 可能为空
    append_log(f"[DEBUG] event corp={corp_in_msg} agentid={agent_in_msg}")

    msg_type = get_tag_text(doc, "MsgType").lower()

    # 1) 客服事件优先
    if msg_type == "event":
        if handle_kf_event(doc):
            return "success"
        return "success"

    # 2) 普通应用文本（内部成员 → 应用）
    if msg_type == "text":
        user_id = get_tag_text(doc, "FromUserName")
        content = get_tag_text(doc, "Content")
        if user_id and content:
            prefix = "[企业同事]" if not user_id.startswith("wm") else "[微信用户]"
            log_and_forward(user_id, f"{prefix} {content}", CHANNEL_APP, 0)

    return "success"

# ============== 调试接口 ==============
@app.route("/debug/kf_accounts")
def debug_kf_accounts():
    """查看当前 access_token 能管理的客服账号列表"""
    try:
        token = get_kf_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/account/list?access_token={token}"
        r = requests.get(url, timeout=10); r.raise_for_status()
        data = r.json()
        append_log(f"[INFO] kf/account/list: {data}")
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/debug/ping")
def debug_ping():
    return "ok", 200

# ============== 启动 ==============
if __name__ == "__main__":
    # 启动定时补拉线程（自愈）
    threading.Thread(target=periodic_sync_loop, daemon=True).start()
    # 生产建议用 gunicorn/uwsgi + systemd（或 nohup 跑也行）
    app.run("0.0.0.0", 5000)
