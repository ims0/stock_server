#!/bin/bash
##########################################################
# File Name: run.sh
# Author: ims
# Created Time: Sun Mar 22 00:03:20 2026
##########################################################


# 1. 先安装虚拟环境工具（如果没装）
#sudo apt install -y python3-venv

# 2. 创建项目目录并进入（替换成你的项目名）
#mkdir my_akshare_project && cd my_akshare_project

python3 -m venv venv

# 4. 激活虚拟环境（关键！激活后命令行开头会显示 (venv)）
source venv/bin/activate

export TUSHARE_TOKEN="f028f82a7bd86c57e54607995b4ed38b7eb3894e357a882eb7a5f665"


# 5. 在虚拟环境中安装 akshare（此时用 pip 而非 pip3，且无权限问题）
#pip install akshare


python3 app.py
