# command.py
import sys
import datetime
import requests
from xml.etree import ElementTree

# ===== 配置区域 =====
USERNAME = "pi_1412@163.com"
PASSWORD = "a8ssjb53qgtcjqvq"
WEBDAV_URL = "https://dav.jianguoyun.com/dav/piri的收集箱/Inbox/"


def generate_markdown(content, existing_content=None):
    """生成Markdown内容（支持合并模式）"""
    time_str = datetime.datetime.now().strftime("%H:%M")
    new_line = f"- {time_str} {content}"
    return f"{new_line}\n{existing_content}" if existing_content else f"{new_line}\n"

def check_file_exists(filename):
    """检查WebDAV文件是否存在"""
    url = WEBDAV_URL + filename
    try:
        response = requests.request("PROPFIND", url, auth=(USERNAME, PASSWORD), headers={"Depth": "0"})
        return response.status_code == 207
    except requests.exceptions.RequestException:
        return False

def get_existing_content(filename):
    """获取现有文件内容"""
    url = WEBDAV_URL + filename
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    return response.text if response.status_code == 200 else None

def upload_to_webdav(filename, content):
    """上传到坚果云WebDAV"""
    url = WEBDAV_URL + filename
    response = requests.put(url, data=content.encode("utf-8"), auth=(USERNAME, PASSWORD))
    if response.status_code in (200, 201, 204):
        print(f"✅ 上传成功：{filename}")
    else:
        print(f"❌ 上传失败：{response.status_code} - {response.text}")

def main():
    if len(sys.argv) < 5:
        print("Usage: python3 command.py <sender> <content> <channel> <msg_type>")
        return

    sender = sys.argv[1]      # 发送者ID
    content = sys.argv[2]     # 消息内容
    channel = sys.argv[3]     # 通道号（未使用）
    msg_type = sys.argv[4]    # 消息类型（0=文本，1=图片）

    # 按日期生成文件名（如 2023-10-01.md）
    filename = f"{datetime.date.today().isoformat()}.md"
    
    # 合并模式处理
    existing_content = None
    if check_file_exists(filename):
        existing_content = get_existing_content(filename)
    
    # 生成Markdown内容
    markdown_content = generate_markdown(content, existing_content)
    
    # 上传到坚果云
    upload_to_webdav(filename, markdown_content)

if __name__ == "__main__":
    main()