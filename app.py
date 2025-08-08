# -*- coding: utf-8 -*-
"""
企业微信「微信客服 ↔ 自建应用」统一回调程序（含自愈补拉 + 保存成功回执）
- 回调 URL： http(s)://你的域名/hook_path
- 客服绑定：客服 → 通过自建应用管理此账号 → 绑定到本应用
- 支持消息：text, link（公众号/知乎等）
- 自愈：periodic_sync_loop() 每 SYNC_INTERVAL 秒按游标补拉
- 回执：command.py 成功时回“保存成功✅”，失败时回“保存失败❌”
"""

from flask import Flask, request, jsonify
from xml.dom.minidom import parseString
import time, os, sys, json, threading, subprocess
import requests
from datetime import datetime

# ============== 加解密依赖 ==============
sys.path.append("weworkapi_python/callback")  # 确保 WXBizMsgCrypt3.py 可用
from WXBizMsgCrypt3 import WXBizMsgCrypt

app = Flask(__name__)

# ============== 配置读取（用环境变量注入） ==============
APP_TOKEN = os.environ.get("APP_TOKEN", "")
APP_ENCODING_AES_KEY = os.environ.get("APP_ENCODING_AES_KEY", "")
CORP_ID = os.environ.get("CORP_ID", "")

# 取 access_token 的密钥（两者都填则优先 KF_SECRET）
KF_SECRET = os.environ.get("KF_SECRET", "")
APP_SECRET = os.environ.get("APP_SECRET", "")

# 你的转发脚本（保持你自己的）
COMMAND_PY = os.environ.get("COMMAND_PY", "command.py")

# 运行参数
CURSOR_FILE = os.environ.get("CURSOR_FILE", "kf_cursor.json")   # {OpenKfId: cursor}
SEEN_FILE   = os.environ.get("SEEN_FILE", "kf_seen.json")       # {OpenKfId: [msgid...]}
LOG_FILE    = os.environ.get("LOG_FILE", "history.log")
CHANNEL_KF  = int(os.environ.get("CHANNEL_KF", "9"))            # 客服来源
CHANNEL_APP = int(os.environ.get("CHANNEL_APP", "0"))           # 应用来源（内部）
POLL_LIMIT  = int(os.environ.get("POLL_LIMIT", "1000"))         # 每次拉取上限
SYNC_INTERVAL = int(os.environ.get("SYNC_INTERVAL", "60"))      # 自愈补拉间隔（秒）
CMD_TIMEOUT = int(os.environ.get("CMD_TIMEOUT", "30"))          # command.py 超时（秒）
# ===================================================

# 基本校验
if not (APP_TOKEN and APP_ENCODING_AES_KEY and CORP_ID):
    print("[WARN] APP_TOKEN/APP_ENCODING_AES_KEY/CORP_ID 未配置，回调校验将失败。")

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

def md_escape(text: str) -> str:
    """转义可能破坏 Markdown 的括号/方括号"""
    return (text or "").replace("[", r"\[").replace("]", r"\]").replace("(", r"\)").replace(")", r"\)")

def md_link(title: str, url: str) -> str:
    """安全的 Markdown 链接：[{title}](<url>)"""
    return f"[{md_escape(title) or '链接'}](<{url.strip()}>)"

def get_kf_access_token():
    """
    优先 KF_SECRET；若缺失/失败则用 APP_SECRET。
    注意：若走 APP_SECRET，需在自建应用的 API 权限里勾选“微信客服”能力。
    """
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

def load_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)

def load_cursors(): return load_json(CURSOR_FILE, {})
def save_cursors(cursors: dict): save_json(CURSOR_FILE, cursors)
def load_seen(): return load_json(SEEN_FILE, {})
def save_seen(seen: dict): save_json(SEEN_FILE, seen)

# ========= 主动回消息（保存成功/失败回执） =========
def send_kf_text(open_kfid: str, touser: str, text: str):
    """
    给客服会话里某个用户回一条文本
    - open_kfid: 客服账号ID（wk...）
    - touser:    用户external_userid（wm...）
    """
    try:
        token = get_kf_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/send_msg?access_token={token}"
        payload = {
            "touser": touser,
            "open_kfid": open_kfid,
            "msgtype": "text",
            "text": {"content": text[:2000]},
        }
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        if data.get("errcode") != 0:
            append_log(f"[WARN] send_kf_text failed: {data}")
        return data
    except Exception as e:
        append_log(f"[ERR] send_kf_text exception: {e}")
        return {"errcode": -1, "errmsg": str(e)}

# ========= 统一转发（同步执行 command.py + 回执） =========
def log_and_forward(user_id: str, content: str, channel: int, msg_type: int, open_kfid: str = None):
    """msg_type: 0=文本（link 也整理成文本内容转发）"""
    append_log(f"[ch{channel}] {user_id}: {content[:200]}")
    ok = False
    try:
        cp = subprocess.run(
            ["python3", COMMAND_PY, str(user_id), str(content), str(channel), str(msg_type)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=CMD_TIMEOUT
        )
        ok = (cp.returncode == 0)
        if not ok:
            append_log(f"[ERR] command.py rc={cp.returncode}, stderr={cp.stderr[-400:]}")
    except subprocess.TimeoutExpired:
        append_log("[ERR] command.py timeout")
    except Exception as e:
        append_log(f"[ERR] start/run command.py failed: {e}")

    # 仅外部微信用户 + 有 open_kfid 时回执
    if open_kfid and str(user_id).startswith("wm"):
        tip = "保存成功✅" if ok else "保存失败❌（稍后再试）"
        send_kf_text(open_kfid, user_id, tip)

def get_tag_text(doc, tag_name):
    nodes = doc.getElementsByTagName(tag_name)
    if not nodes or not nodes[0].childNodes:
        return ""
    return (nodes[0].childNodes[0].data or "").strip()

# ========= 拉取并处理客服消息 =========
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

        user = m.get("external_userid", "wx_external")

        # 1) 文本
        if m.get("msgtype") == "text":
            got_any = True
            text = (m.get("text") or {}).get("content", "").strip()
            # 针对“不可展示”的提醒
            if text.strip() == "[该消息类型暂不能展示]":
                tip = "提示：该消息类型客服接口不支持。请直接粘贴链接或发截图~"
                log_and_forward(user, f"{tip}", CHANNEL_KF, 0, open_kfid=open_kfid)
                if msgid: seen_set.add(msgid)
                continue
            if text:
                content = f"{text}"
                log_and_forward(user, content, CHANNEL_KF, 0, open_kfid=open_kfid)
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
                md = md_link(title, url_)
                # 你也可以只保留一行 md，不要描述：
                content = f"{md}"
                # content = f"{md}\n{desc}"
                log_and_forward(user, content, CHANNEL_KF, 0, open_kfid=open_kfid)
                if msgid: seen_set.add(msgid)


        # 其他类型暂不处理（image/voice/file/miniprogram 可后续扩展）

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

# ============== 可见客服账号列表（自愈用） ==============
def list_kf_accounts():
    try:
        token = get_kf_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/kf/account/list?access_token={token}"
        r = requests.get(url, timeout=10); r.raise_for_status()
        data = r.json()
        if data.get("errcode") != 0:
            append_log(f"[ERR] kf/account/list: {data}")
            return []
        return [i.get("open_kfid") for i in (data.get("account_list") or []) if i.get("open_kfid")]
    except Exception as e:
        append_log(f"[ERR] list_kf_accounts exception: {e}")
        return []

# 避免同一 kfid 并发同步（事件 + 定时器同时触发）
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

    corp_in_msg  = get_tag_text(doc, "ToUserName")  # 企业ID
    agent_in_msg = get_tag_text(doc, "AgentID")     # 有些事件为空
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
            # 内部消息不需要回执，open_kfid 传 None
            log_and_forward(user_id, f"{prefix} {content}", CHANNEL_APP, 0, open_kfid=None)

    return "success"

# ============== 调试接口 ==============
@app.route("/debug/kf_accounts")
def debug_kf_accounts():
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
    # 生产建议用 gunicorn/uwsgi + systemd（或 nohup 也行）
    app.run("0.0.0.0", 5000)
