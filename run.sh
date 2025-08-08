#!/bin/bash
export APP_TOKEN=""
export APP_ENCODING_AES_KEY=""
export CORP_ID=""

# 取 access_token 的密钥（填其一即可；两者都填则优先 KF_SECRET）
export KF_SECRET=""  # 若能在“微信客服/开发配置”里拿到 Secret，填这里（推荐）
export APP_SECRET=""

# 启动程序
python app.py >> app.out 2>&1 &
echo $! > app.pid