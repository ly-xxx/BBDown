import asyncio
import json
import os
import csv
import requests
import subprocess
import glob
import random
import time
import sys
import traceback
import re
import pickle
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
from bs4 import BeautifulSoup
import shutil

# 添加进度条显示
def print_progress(current, total, title, length=50):
    """打印进度条"""
    percent = current / total
    filled_length = int(length * percent)
    bar = '█' * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{title}: |{bar}| {percent:.1%} ({current}/{total})')
    sys.stdout.flush()
    if current == total:
        print()

# 生成按周的时间范围
def generate_weekly_ranges(start_date_str="2023-04-22", end_date_str="2025-06-06"):
    """生成从起始日期到结束日期的每周时间戳范围"""
    # 将字符串转换为日期对象
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # 存储所有周的开始和结束时间戳
    weekly_ranges = []
    
    # 从起始日期开始
    current_date = start_date
    while current_date < end_date:
        # 计算当前周的结束日期（下一周的开始前一天）
        week_end_date = current_date + timedelta(days=6)
        
        # 如果结束日期超过了总的结束日期，使用总的结束日期
        if week_end_date > end_date:
            week_end_date = end_date
        
        # 转换为UNIX时间戳（秒）
        begin_timestamp = int(current_date.timestamp())
        end_timestamp = int((week_end_date + timedelta(days=1)).timestamp() - 1)  # 减1秒，以获取当天的23:59:59
        
        # 添加到列表中
        weekly_ranges.append({
            'begin_date': current_date.strftime("%Y-%m-%d"),
            'end_date': week_end_date.strftime("%Y-%m-%d"),
            'begin_timestamp': begin_timestamp,
            'end_timestamp': end_timestamp
        })
        
        # 移到下一周
        current_date = week_end_date + timedelta(days=1)
    
    return weekly_ranges

# Add log file for debugging
log_file = "crawler_log.txt"
with open(log_file, "w", encoding="utf-8") as f:
    f.write(f"Starting 哈基米 video crawler at {datetime.now()}\n")
    f.write(f"Python version: {sys.version}\n")

print("Starting 哈基米 video crawler...")
print(f"Python version: {sys.version}")

try:
    class BilibiliCrawler:
        def __init__(self):
            self.base_url = "https://api.bilibili.com"
            self.search_url = "https://search.bilibili.com/all"
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67"
            ]
            self.headers = self._get_random_headers()
            self.output_dir = "hachimi_videos"  # Use relative path
            self.max_duration = 600  # 10 minutes in seconds
            self.search_keyword = "哈基米"
            self.csv_file = os.path.join(self.output_dir, "video_info.csv") # Path becomes relative due to self.output_dir
            self.ffmpeg_path = os.path.join("ffmpeg-7.1.1-full_build", "bin", "ffmpeg.exe") # Use relative path
            self.bbdown_path = "BBDown.exe"  # Use relative path, expects BBDown.exe in CWD or PATH
            self.max_retries = 3
            self.min_view_count = 500  # 修改为500播放量起步
            self.failed_csv_file = os.path.join(self.output_dir, "failed_downloads.csv") # Path becomes relative due to self.output_dir
            
            # 会话状态保存路径
            self.session_file = "bili_session.pickle" # Use relative path, stored in CWD
            
            # 登录状态
            self.is_logged_in = False
            
            # 获取时间范围
            self.weekly_ranges = generate_weekly_ranges()
            self.log(f"共生成 {len(self.weekly_ranges)} 个周时间范围")
            
            # 创建会话
            self.session = self._get_session()
            
            # 存储已下载视频的BV号
            self.downloaded_videos = self._load_downloaded_videos()
            
            # Create directories
            self.create_directories()
            
            # 初始化登录
            self.login()
            
        def login(self):
            """登录B站账号"""
            self.log("准备登录B站账号...")
            
            # 尝试加载保存的会话
            if self._load_session():
                self.log("已从保存的会话中恢复登录状态！")
                # 验证登录状态是否有效
                if self._verify_login():
                    self.log("登录状态有效，可以继续操作")
                    self.is_logged_in = True
                    return True
                else:
                    self.log("登录状态已失效，需要重新登录")
            
            # 如果没有有效的会话，执行登录流程
            self.log("没有找到有效的会话数据，需要重新登录")
            
            # 首先尝试手动输入SESSDATA
            self.log("您可以选择手动提供SESSDATA")
            print("\n" + "="*50)
            print("请手动提供SESSDATA (或直接按回车跳过，使用扫码登录)：")
            print("1. 打开浏览器，访问bilibili.com并确保已登录")
            print("2. 按F12打开开发者工具，切换到'应用'或'Application'标签")
            print("3. 在左侧找到'Cookies'，然后找到bilibili.com")
            print("4. 在右侧找到名为'SESSDATA'的cookie，复制其值")
            print("5. 粘贴到下方并按回车")
            print("="*50)
            
            try:
                sessdata_input = input("SESSDATA (直接按回车跳过): ").strip()
                if sessdata_input:
                    # 创建会话数据
                    session_data = {
                        'cookies': {
                            'SESSDATA': sessdata_input
                        },
                        'timestamp': datetime.now().timestamp(),
                        'user_agent': self.session.headers.get('User-Agent')
                    }
                    
                    # 更新当前会话
                    self.session.cookies.set('SESSDATA', sessdata_input, domain='.bilibili.com')
                    
                    # 保存会话数据
                    with open(self.session_file, 'wb') as f:
                        pickle.dump(session_data, f)
                    
                    self.log(f"手动输入的SESSDATA已保存")
                    
                    # 验证登录是否有效
                    if self._verify_login():
                        self.log("登录验证成功！")
                        # 同时更新BBDown的cookie
                        try:
                            bbdown_cookie_dir = os.path.join(os.path.expanduser("~"), ".bbdown")
                            os.makedirs(bbdown_cookie_dir, exist_ok=True)
                            cookie_file = os.path.join(bbdown_cookie_dir, "cookies.json")
                            with open(cookie_file, 'w', encoding='utf-8') as f:
                                json.dump({'SESSDATA': sessdata_input}, f)
                            self.log("已更新BBDown的cookie文件")
                        except Exception as e:
                            self.log(f"更新BBDown cookie时出错: {e}")
                        self.is_logged_in = True
                        return True
                    else:
                        self.log("登录验证失败，提供的SESSDATA可能无效，将尝试扫码登录")
            except Exception as e:
                self.log(f"处理手动输入时出错: {e}")
            
            # 如果手动输入失败或用户选择跳过，尝试扫码登录
            self.log("检查BBDown登录状态...")
            
            # 重试登录，直到成功
            max_login_attempts = 3
            for attempt in range(max_login_attempts):
                try:
                    # 运行BBDown登录命令，直接显示二维码，不捕获输出
                    self.log(f"请扫描二维码登录B站账号...（尝试 {attempt+1}/{max_login_attempts}）")
                    self.log("请注意：二维码将直接在控制台窗口显示，请勿关闭窗口")
                    
                    # 直接运行BBDown登录命令，显示二维码
                    subprocess.run([self.bbdown_path, "login"], check=True)
                    
                    # 登录成功后，尝试从BBDown的cookie文件中获取SESSDATA
                    bbdown_cookie_dir = os.path.join(os.path.expanduser("~"), ".bbdown")
                    cookie_file = os.path.join(bbdown_cookie_dir, "cookies.json")
                    
                    if os.path.exists(cookie_file):
                        try:
                            with open(cookie_file, 'r', encoding='utf-8') as f:
                                cookies_data = json.load(f)
                            
                            # 检查是否包含SESSDATA
                            if 'SESSDATA' in cookies_data:
                                sessdata = cookies_data['SESSDATA']
                                self.log(f"成功从BBDown cookie文件中获取SESSDATA")
                                
                                # 保存会话数据
                                session_data = {
                                    'cookies': cookies_data,
                                    'timestamp': datetime.now().timestamp(),
                                    'user_agent': self.session.headers.get('User-Agent')
                                }
                                
                                # 更新当前会话的cookie
                                for key, value in cookies_data.items():
                                    self.session.cookies.set(key, value, domain='.bilibili.com')
                                
                                # 保存会话数据
                                with open(self.session_file, 'wb') as f:
                                    pickle.dump(session_data, f)
                                
                                self.log(f"会话数据已保存到 {self.session_file}")
                                
                                # 验证登录是否有效
                                if self._verify_login():
                                    self.log("登录验证成功！")
                                    self.is_logged_in = True
                                    return True
                                else:
                                    self.log("登录可能成功但验证失败，尝试重新登录...")
                                    continue
                            else:
                                self.log("BBDown cookie文件中未找到SESSDATA")
                        except Exception as e:
                            self.log(f"读取BBDown cookie文件时出错: {e}")
                    else:
                        self.log(f"BBDown cookie文件不存在: {cookie_file}")
                        
                except subprocess.CalledProcessError as e:
                    self.log(f"登录失败: {e}")
                    self.log("请检查网络连接或BBDown是否正常工作")
                    time.sleep(3)  # 等待一段时间后重试
            
            self.log("多次尝试登录失败，请检查BBDown安装是否正确")
            return False
            
        def _verify_login(self):
            """验证登录状态是否有效"""
            try:
                # 首先检查会话中是否有SESSDATA
                if 'SESSDATA' not in self.session.cookies:
                    self.log("会话中没有SESSDATA，无法验证登录")
                    return False
                
                # 访问需要登录的API
                test_url = "https://api.bilibili.com/x/web-interface/nav"
                self.log(f"验证登录状态，访问: {test_url}")
                
                headers = self._get_random_headers()
                self.log(f"使用User-Agent: {headers['User-Agent'][:30]}...")
                
                # 显示请求cookie
                cookies_dict = dict(self.session.cookies)
                for k, v in cookies_dict.items():
                    if k == 'SESSDATA':
                        self.log(f"请求Cookie - {k}: {v[:10]}...")
                    else:
                        self.log(f"请求Cookie - {k}: {v}")
                
                response = self.session.get(test_url, headers=headers, timeout=10)
                
                self.log(f"API响应状态码: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    self.log(f"API响应: {json.dumps(data, ensure_ascii=False)[:200]}...")
                    
                    if data.get("code") == 0 and data.get("data", {}).get("isLogin"):
                        self.log(f"登录验证成功，用户名: {data.get('data', {}).get('uname')}")
                        # 保存最新的cookie
                        try:
                            self._save_session()
                            self.log("已更新会话数据")
                        except Exception as e:
                            self.log(f"更新会话数据时出错: {e}")
                        return True
                    else:
                        error_msg = data.get('message', '未知错误')
                        code = data.get('code', -1)
                        self.log(f"登录验证失败，API返回: {error_msg} (代码: {code})")
                else:
                    self.log(f"登录验证失败，状态码: {response.status_code}")
                    try:
                        self.log(f"错误响应: {response.text[:200]}...")
                    except:
                        pass
                
                return False
            except requests.exceptions.Timeout:
                self.log("验证登录状态超时，网络连接可能不稳定")
                return False
            except requests.exceptions.ConnectionError:
                self.log("验证登录状态时连接错误，网络可能断开")
                return False
            except Exception as e:
                self.log(f"验证登录状态时出错: {e}")
                traceback.print_exc()
                return False
        
        def _save_session(self, sessdata=None):
            """保存会话数据"""
            try:
                # 如果提供了SESSDATA直接使用
                if sessdata:
                    session_data = {
                        'cookies': {
                            'SESSDATA': sessdata
                        },
                        'timestamp': datetime.now().timestamp(),
                        'user_agent': self.session.headers.get('User-Agent')
                    }
                    
                    with open(self.session_file, 'wb') as f:
                        pickle.dump(session_data, f)
                        
                    self.log(f"会话数据已保存到 {self.session_file}")
                    return True
                    
                # 尝试获取BBDown的cookie文件
                cookie_file = os.path.join(os.path.expanduser("~"), ".bbdown", "cookies.json")
                if os.path.exists(cookie_file):
                    with open(cookie_file, 'r', encoding='utf-8') as f:
                        cookies_data = json.load(f)
                        
                    # 保存会话数据
                    session_data = {
                        'cookies': cookies_data,
                        'timestamp': datetime.now().timestamp(),
                        'user_agent': self.session.headers.get('User-Agent')
                    }
                    
                    with open(self.session_file, 'wb') as f:
                        pickle.dump(session_data, f)
                        
                    self.log(f"会话数据已保存到 {self.session_file}")
                    return True
                else:
                    # 直接创建简单的会话数据
                    session_data = {
                        'cookies': {},
                        'timestamp': datetime.now().timestamp(),
                        'user_agent': self.session.headers.get('User-Agent')
                    }
                    
                    with open(self.session_file, 'wb') as f:
                        pickle.dump(session_data, f)
                        
                    self.log("未找到BBDown的cookie文件，已创建基本会话数据")
                    return True
            except Exception as e:
                self.log(f"保存会话数据时出错: {e}")
                return False
                
        def _load_session(self):
            """加载保存的会话数据"""
            try:
                if not os.path.exists(self.session_file):
                    self.log("没有找到保存的会话文件")
                    return False
                
                self.log(f"尝试从 {self.session_file} 加载会话数据")
                    
                with open(self.session_file, 'rb') as f:
                    session_data = pickle.load(f)
                
                # 输出调试信息
                self.log(f"会话数据时间戳: {datetime.fromtimestamp(session_data.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S')}")
                cookies_data = session_data.get('cookies', {})
                self.log(f"找到 {len(cookies_data)} 个cookie")
                
                # 检查会话是否过期（7天有效期）
                timestamp = session_data.get('timestamp', 0)
                current_ts = datetime.now().timestamp()
                session_age = current_ts - timestamp
                session_days = session_age / (24 * 3600)
                
                self.log(f"会话年龄: {session_days:.2f} 天")
                
                if session_age > 7 * 24 * 3600:
                    self.log(f"保存的会话已过期 (超过7天)")
                    return False
                    
                # 恢复cookie到会话
                if cookies_data:
                    # 检查是否有SESSDATA
                    if 'SESSDATA' in cookies_data:
                        sessdata = cookies_data['SESSDATA']
                        # 不再打印SESSDATA的任何部分
                        self.log("找到SESSDATA并成功加载")
                        self.session.cookies.set('SESSDATA', sessdata, domain='.bilibili.com')
                        self.log("成功加载SESSDATA到会话")
                    else:
                        self.log("会话数据中没有SESSDATA")
                        return False
                    
                    # 如果有其他cookie也加载
                    for key, value in cookies_data.items():
                        if key != 'SESSDATA':
                            self.session.cookies.set(key, value, domain='.bilibili.com')
                            self.log(f"加载cookie: {key}")
                    
                    # 更新当前会话的User-Agent
                    if 'user_agent' in session_data:
                        user_agent = session_data['user_agent']
                        self.log(f"使用保存的User-Agent: {user_agent[:30]}...")
                        self.session.headers.update({'User-Agent': user_agent})
                        
                    # 如果存在BBDown的cookie目录，也更新BBDown的cookie
                    bbdown_cookie_dir = os.path.join(os.path.expanduser("~"), ".bbdown")
                    if not os.path.exists(bbdown_cookie_dir):
                        self.log(f"创建BBDown cookie目录: {bbdown_cookie_dir}")
                        os.makedirs(bbdown_cookie_dir, exist_ok=True)
                        
                    cookie_file = os.path.join(bbdown_cookie_dir, "cookies.json")
                    try:
                        self.log(f"更新BBDown cookie文件: {cookie_file}")
                        with open(cookie_file, 'w', encoding='utf-8') as f:
                            json.dump(cookies_data, f)
                        self.log("成功更新BBDown的cookie文件")
                    except Exception as e:
                        self.log(f"更新BBDown的cookie文件失败: {e}")
                    
                    self.is_logged_in = True
                    self.log("成功加载保存的会话数据")
                    return True
                else:
                    self.log("会话数据中没有任何cookie")
                return False
            except Exception as e:
                self.log(f"加载会话数据时出错: {e}")
                traceback.print_exc()
                return False
        
        def _load_downloaded_videos(self) -> Set[str]:
            """从CSV文件和文件系统加载已下载的视频"""
            downloaded_bvids = set()
            
            # 1. 从CSV文件加载
            if os.path.exists(self.csv_file):
                try:
                    with open(self.csv_file, 'r', encoding='utf-8') as f:
                        # 检查是否有表头
                        first_line = f.readline()
                        f.seek(0)
                        if 'bvid' in first_line:
                            # 有表头，使用DictReader
                            reader = csv.DictReader(f)
                            for row in reader:
                                bvid = row.get('bvid', '').strip()
                                if bvid:
                                    downloaded_bvids.add(bvid)
                            self.log(f"从CSV文件加载了 {len(downloaded_bvids)} 个已下载视频")
                        else:
                            # 无表头，按首列处理
                            reader = csv.reader(f)
                            for row in reader:
                                if row and row[0].startswith('BV'):
                                    downloaded_bvids.add(row[0].strip())
                            self.log(f"从无表头CSV加载了 {len(downloaded_bvids)} 个已下载视频")
                except Exception as e:
                    self.log(f"读取CSV文件时出错: {e}")
            
            # 2. 从文件系统扫描目录名中的BV号
            for root, dirs, files in os.walk(self.output_dir):
                for file in files:
                    if file.endswith("_final.mp4"):
                        # 尝试从文件名中提取BV号
                        bv_match = re.search(r'(BV[a-zA-Z0-9]{10})_.*final\.mp4', file)
                        if bv_match:
                            downloaded_bvids.add(bv_match.group(1))
            
            self.log(f"总共找到 {len(downloaded_bvids)} 个已下载视频")
            return downloaded_bvids
        
        def _get_session(self):
            """Create a session with cookies and headers"""
            session = requests.Session()
            
            # Set some default cookies that make us look like a regular browser
            cookies = {
                'buvid3': f'{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=32))}_{"".join(random.choices("0123456789", k=10))}infoc',
                'innersign': '0',
                'b_nut': str(int(datetime.now().timestamp())),  # Use timestamp() instead of strftime('%s')
                'i-wanna-go-back': '-1',
                'b_ut': '5',
                'b_lsid': f'{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=16))}',
                'bsource': 'search_google',
                '_uuid': f'{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=32))}infoc',
                'CURRENT_BLACKGAP': '0',
                'buvid4': f'{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=32))}-{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=16))}-{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=16))}-{"".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=16))}',
                'sid': '{"".join(random.choices("0123456789", k=8))}',
            }
            
            for key, value in cookies.items():
                session.cookies.set(key, value)
                
            # Add headers to the session
            session.headers.update(self._get_random_headers())
            
            return session
            
        def _get_random_headers(self) -> Dict[str, str]:
            """Generate random headers to avoid detection"""
            user_agent = random.choice(self.user_agents)
            return {
                "User-Agent": user_agent,
                "Referer": "https://www.bilibili.com/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "DNT": "1"
            }
            
        def log(self, message):
            """Log a message to the log file and print it"""
            print(message)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()}: {message}\n")
            
        def random_sleep(self, min_seconds=1, max_seconds=3):
            """Sleep for a random amount of time to avoid detection"""
            sleep_time = random.uniform(min_seconds, max_seconds)
            self.log(f"Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
        def create_directories(self):
            # Create main output directory
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Create subdirectories for view count ranges
            view_ranges = [
                "1M+",
                "500K-1M",
                "100K-500K", 
                "10K-100K",
                "500-10K"  # 修改为500起步
            ]
            for view_range in view_ranges:
                os.makedirs(os.path.join(self.output_dir, view_range), exist_ok=True)
        
        async def search_videos_by_time_range(self, time_range, page=1, page_size=20) -> Dict:
            """按时间范围搜索视频"""
            # 对于B站搜索，我们改为使用不带时间过滤的搜索，然后手动过滤
            base_url = "https://search.bilibili.com/all"
            
            # 构建基本URL参数
            url_params = {
                "keyword": self.search_keyword,
                "page": str(page)
            }
            
            # 构建完整URL
            param_str = "&".join([f"{k}={v}" for k, v in url_params.items()])
            full_url = f"{base_url}?{param_str}"
            self.log(f"搜索URL: {full_url}")
            
            for attempt in range(self.max_retries):
                try:
                    # 刷新会话
                    if attempt > 0:
                        self.log(f"搜索重试 {attempt+1}/{self.max_retries}")
                        self.session = self._get_session()
                    
                    # 模拟浏览器直接访问完整URL
                    time_range_str = f"时间范围: {time_range['begin_date']}~{time_range['end_date']}"
                    self.log(f"访问搜索页面: 第{page}页，{time_range_str}")
                    
                    # 构建请求头
                    headers = self._get_random_headers()
                    # 添加额外的请求头
                    headers.update({
                        "Referer": "https://search.bilibili.com/",
                    })
                    
                    response = self.session.get(full_url, headers=headers, timeout=15)
                    
                    self.log(f"搜索响应状态码: {response.status_code}")
                    
                    # 保存网页内容用于调试
                    debug_dir = os.path.join(self.output_dir, "debug")
                    os.makedirs(debug_dir, exist_ok=True)
                    date_str = time_range['begin_date']
                    debug_file = os.path.join(debug_dir, f"search_all_{date_str}_{page}.html")
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    self.log(f"已保存搜索结果到: {debug_file}")
                    
                    # 随机延迟
                    self.random_sleep()
                    
                    if response.status_code == 200:
                        # 解析所有视频
                        all_videos = self.parse_search_results(response.text)
                        
                        if all_videos["videos"]:
                            self.log(f"找到 {len(all_videos['videos'])} 个视频，准备按日期过滤")
                            
                            # 手动过滤时间范围
                            filtered_videos = []
                            for video in all_videos["videos"]:
                                # 尝试解析上传时间
                                pub_time = video.get("pub_time", "")
                                pub_timestamp = self.parse_pub_time(pub_time, time_range)
                                
                                # 如果能够解析出时间，且在范围内
                                if pub_timestamp and time_range['begin_timestamp'] <= pub_timestamp <= time_range['end_timestamp']:
                                    video['pub_timestamp'] = pub_timestamp
                                    filtered_videos.append(video)
                            
                            self.log(f"时间过滤后剩余 {len(filtered_videos)} 个视频")
                            return {"videos": filtered_videos, "has_next_page": all_videos["has_next_page"]}
                        else:
                            self.log(f"未找到视频")
                            return {"videos": [], "has_next_page": False}
                    elif response.status_code == 412:  # 反爬虫保护
                        self.log("检测到反爬虫保护，等待更长时间后重试...")
                        self.random_sleep(10, 20)  # 更长的等待时间
                    else:
                        self.log(f"请求失败，状态码: {response.status_code}")
                        self.random_sleep(3, 7)
                except Exception as e:
                    self.log(f"搜索过程中出现异常: {e}")
                    traceback.print_exc()
                    self.random_sleep(5, 10)
            
            self.log("达到最大重试次数，搜索失败")
            return {"videos": [], "has_next_page": False}
        
        def parse_pub_time(self, pub_time_str, time_range):
            """解析视频发布时间为时间戳"""
            try:
                # 处理B站常见的时间格式
                current_year = datetime.now().year
                
                # 具体日期格式：5-27、5-30、5-31等
                if re.match(r'\d+-\d+', pub_time_str):
                    # 补充年份
                    if time_range['begin_date'].startswith('2025'):
                        full_date = f"2025-{pub_time_str}"
                    elif time_range['begin_date'].startswith('2024'):
                        full_date = f"2024-{pub_time_str}"
                    elif time_range['begin_date'].startswith('2023'):
                        full_date = f"2023-{pub_time_str}"
                    elif time_range['begin_date'].startswith('2022'):
                        full_date = f"2022-{pub_time_str}"
                    else:
                        full_date = f"{current_year}-{pub_time_str}"
                    
                    # 转换为时间戳
                    dt = datetime.strptime(full_date, "%Y-%m-%d")
                    return int(dt.timestamp())
                
                # 处理"昨天"、"前天"
                if "昨天" in pub_time_str:
                    yesterday = datetime.now() - timedelta(days=1)
                    return int(yesterday.timestamp())
                if "前天" in pub_time_str:
                    day_before_yesterday = datetime.now() - timedelta(days=2)
                    return int(day_before_yesterday.timestamp())
                
                # 处理"xx小时前"、"xx分钟前"
                hours_match = re.search(r'(\d+)小时前', pub_time_str)
                if hours_match:
                    hours = int(hours_match.group(1))
                    timestamp = int(datetime.now().timestamp()) - hours * 3600
                    return timestamp
                
                minutes_match = re.search(r'(\d+)分钟前', pub_time_str)
                if minutes_match:
                    minutes = int(minutes_match.group(1))
                    timestamp = int(datetime.now().timestamp()) - minutes * 60
                    return timestamp
                
                # 无法解析的情况
                return None
            except Exception as e:
                self.log(f"解析发布时间出错: {e} - {pub_time_str}")
                return None
        
        def parse_search_results(self, html_content) -> Dict:
            """从搜索结果HTML中解析视频信息"""
            soup = BeautifulSoup(html_content, 'html.parser')
            videos = []
            has_next_page = False
            
            # 查找分页按钮，检查是否有下一页
            pagination = soup.select('.vui_pagenation--btn-next')
            if pagination and not "disabled" in pagination[0].get("class", []):
                has_next_page = True
            
            # 查找视频卡片
            video_cards = soup.select('.bili-video-card')
            
            for card in video_cards:
                try:
                    # 获取BV号
                    href = card.select_one('.bili-video-card__info--right > a')
                    if not href:
                        continue
                    
                    bvid_match = re.search(r'BV[a-zA-Z0-9]{10}', href.get('href', ''))
                    if not bvid_match:
                        continue
                    
                    bvid = bvid_match.group(0)
                    
                    # 获取标题
                    title_elem = card.select_one('.bili-video-card__info--tit')
                    title = title_elem.get('title', '') if title_elem else ''
                    
                    # 获取播放量
                    view_elem = card.select_one('.bili-video-card__stats--item:nth-child(1) span')
                    view_text = view_elem.text if view_elem else '0'
                    view_count = self.parse_count(view_text)
                    
                    # 获取视频时长
                    duration_elem = card.select_one('.bili-video-card__stats__duration')
                    duration_text = duration_elem.text if duration_elem else '00:00'
                    duration = self.parse_duration(duration_text)
                    
                    # 获取上传者信息
                    uploader_elem = card.select_one('.bili-video-card__info--author')
                    uploader = uploader_elem.text.strip() if uploader_elem else '未知'
                    
                    # 获取发布时间
                    time_elem = card.select_one('.bili-video-card__info--date')
                    pub_time = time_elem.text.strip() if time_elem else ''
                    
                    # 获取UP主mid（如果有）
                    mid = None
                    uploader_link = card.select_one('.bili-video-card__info--owner a')
                    if uploader_link:
                        mid_match = re.search(r'space\.bilibili\.com/(\d+)', uploader_link.get('href', ''))
                        if mid_match:
                            mid = mid_match.group(1)
                    
                    videos.append({
                        'bvid': bvid,
                        'title': title,
                        'view_count': view_count,
                        'duration': duration,
                        'uploader': uploader,
                        'mid': mid,
                        'pub_time': pub_time
                    })
                except Exception as e:
                    self.log(f"解析视频卡片时出错: {e}")
            
            return {
                "videos": videos,
                "has_next_page": has_next_page
            }
        
        def parse_count(self, count_text):
            """解析视频播放量文本"""
            try:
                if '万' in count_text:
                    return int(float(count_text.replace('万', '')) * 10000)
                elif '亿' in count_text:
                    return int(float(count_text.replace('亿', '')) * 100000000)
                else:
                    return int(count_text.replace(',', '').strip())
            except:
                return 0
        
        def parse_duration(self, duration_text):
            """解析视频时长文本为秒数"""
            try:
                parts = duration_text.split(':')
                if len(parts) == 2:  # MM:SS
                    return int(parts[0]) * 60 + int(parts[1])
                elif len(parts) == 3:  # HH:MM:SS
                    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                return 0
            except:
                return 0
        
        def get_target_directory(self, view_count):
            """根据播放量确定目标目录"""
            if view_count >= 1000000:
                return os.path.join(self.output_dir, "1M+")
            elif view_count >= 500000:
                return os.path.join(self.output_dir, "500K-1M")
            elif view_count >= 100000:
                return os.path.join(self.output_dir, "100K-500K")
            elif view_count >= 10000:
                return os.path.join(self.output_dir, "10K-100K")
            else:
                return os.path.join(self.output_dir, "500-10K")
        
        def is_video_downloaded(self, bvid) -> bool:
            """检查视频是否已经下载过"""
            # 检查已下载视频集合
            return bvid in self.downloaded_videos
        
        def process_video(self, video_dir, bvid):
            """使用ffmpeg处理视频"""
            # 查找下载的mp4文件
            mp4_files = glob.glob(os.path.join(video_dir, "*.mp4"))
            
            if not mp4_files:
                self.log(f"在 {video_dir} 中未找到MP4文件")
                return False
            
            # 使用找到的第一个mp4文件
            input_file = mp4_files[0]
            
            # 获取目录信息
            dir_name = os.path.basename(video_dir)
            # 获取文件名（不含扩展名）
            base_filename = os.path.splitext(os.path.basename(input_file))[0]
            
            # 构建新的输出文件名，添加_final后缀
            output_filename = f"{base_filename}_final.mp4"
            output_file = os.path.join(video_dir, output_filename)
            
            # 检查如果输出文件已经存在，说明已经处理过了
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                self.log(f"输出文件 {output_file} 已存在，跳过处理")
                # 删除非final的文件
                self._clean_non_final_files(video_dir)
                return True
            
            self.log(f"使用ffmpeg处理视频: {input_file} -> {output_file}")
            
            for attempt in range(self.max_retries):
                try:
                    # 应用ffmpeg处理
                    self.log(f"运行ffmpeg (尝试 {attempt+1}/{self.max_retries})...")
                    
                    # 构建命令
                    cmd = [
                        self.ffmpeg_path,
                        "-i", input_file,
                        "-c:v", "libx264",
                        "-preset", "medium",
                        "-crf", "23",
                        "-c:a", "aac",
                        "-b:a", "128k",
                        "-y",  # 覆盖已存在的文件
                        output_file
                    ]
                    
                    self.log(f"命令: {' '.join(cmd)}")
                    
                    # 运行命令
                    process = subprocess.run(
                        cmd,
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        encoding='utf-8',  # 明确指定编码
                        errors='replace'   # 处理无法解码的字符
                    )
                    
                    # 记录输出
                    if process.stderr:
                        self.log(f"ffmpeg stderr: {process.stderr}")
                    
                    # 处理完成后，删除非final的文件
                    self._clean_non_final_files(video_dir)
                    
                    self.log(f"视频处理完成: {output_file}")
                    return True
                except subprocess.CalledProcessError as e:
                    self.log(f"使用ffmpeg处理视频失败: {e} (尝试 {attempt+1}/{self.max_retries})")
                    self.log(f"命令错误: {e.stderr if hasattr(e, 'stderr') else '无错误输出'}")
                    self.random_sleep(2, 5)
            
            self.log(f"达到最大重试次数，处理失败: {bvid}")
            return False
        
        def _clean_non_final_files(self, directory):
            """删除目录中所有不含final的视频文件"""
            try:
                # 获取目录中所有mp4文件
                all_mp4_files = glob.glob(os.path.join(directory, "*.mp4"))
                
                # 检查是否有final版本
                final_files = [f for f in all_mp4_files if "_final.mp4" in f]
                
                if not final_files:
                    self.log(f"警告：目录 {directory} 中没有找到final版本视频")
                    return False
                
                # 筛选出不含final的文件
                non_final_files = [f for f in all_mp4_files if "_final.mp4" not in f]
                
                # 删除这些文件
                for file in non_final_files:
                    try:
                        os.remove(file)
                        self.log(f"已删除非final文件: {file}")
                    except Exception as e:
                        self.log(f"删除文件 {file} 失败: {e}")
                
                # 检查是否有其他临时文件（如.xml, .m4a等）
                other_temp_files = []
                other_temp_files.extend(glob.glob(os.path.join(directory, "*.xml")))
                other_temp_files.extend(glob.glob(os.path.join(directory, "*.m4a")))
                other_temp_files.extend(glob.glob(os.path.join(directory, "*.flv")))
                other_temp_files.extend(glob.glob(os.path.join(directory, "*.tmp")))
                
                # 删除这些临时文件
                for file in other_temp_files:
                    try:
                        os.remove(file)
                        self.log(f"已删除临时文件: {file}")
                    except Exception as e:
                        self.log(f"删除临时文件 {file} 失败: {e}")
                    
                return True
            except Exception as e:
                self.log(f"清理非final文件时出错: {e}")
                return False
        
        def download_video(self, bvid, mid, author, target_dir, title=""):
            """使用BBDown下载视频"""
            # 检查是否已下载
            if self.is_video_downloaded(bvid):
                self.log(f"视频 {bvid} 已经下载过，跳过")
                return True
                
            # 净化作者名称用于目录
            author = ''.join(c for c in author if c.isalnum() or c in ' -_')
            
            # 在播放量目录内创建上传者目录
            os.makedirs(target_dir, exist_ok=True)
            
            # 构建BBDown命令
            url = f"https://www.bilibili.com/video/{bvid}"
            
            # 获取视频详情来获取发布日期
            api_url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
            try:
                response = self.session.get(api_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    pub_timestamp = data.get("data", {}).get("pubdate", 0)
                    pub_date = datetime.fromtimestamp(pub_timestamp).strftime('%Y%m%d') if pub_timestamp else ''
                    file_pattern = f"{pub_date}_{title}" if pub_date else title
                else:
                    file_pattern = title
            except Exception as e:
                self.log(f"获取视频发布日期出错: {e}")
                file_pattern = title
            
            # 清理文件名模式中的特殊字符
            file_pattern = ''.join(c if c.isalnum() or c in ' _-.' else '_' for c in file_pattern)
            
            self.log(f"下载视频: {url} 到 {target_dir}")
            
            # 使用BBDown下载视频
            for attempt in range(self.max_retries):
                try:
                    self.log(f"运行BBDown (尝试 {attempt+1}/{self.max_retries})...")
                    
                    cmd = [
                        self.bbdown_path,
                        url,
                        "--work-dir", target_dir,
                        "--ffmpeg-path", self.ffmpeg_path,
                        "--file-pattern", file_pattern  # 使用年月日_标题的格式
                    ]
                    
                    self.log(f"命令: {' '.join(cmd)}")
                    
                    # 运行命令，获取字节输出
                    proc = subprocess.run(
                        cmd,
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    
                    # 解码输出
                    try:
                        out = proc.stdout.decode('utf-8')
                    except:
                        out = proc.stdout.decode('gbk', errors='replace')
                    self.log(f"BBDown stdout: {out}")
                    try:
                        err = proc.stderr.decode('utf-8')
                    except:
                        err = proc.stderr.decode('gbk', errors='replace')
                    if err:
                        self.log(f"BBDown stderr: {err}")
                    
                    # 判断已选择的流大小，超过500MB则跳过下载
                    video_size_mb = None
                    for line in out.splitlines():
                        if "[视频]" in line and "~" in line:
                            m = re.search(r"~?([\d\.]+)\s*(GB|MB)", line)
                            if m:
                                num = float(m.group(1))
                                unit = m.group(2)
                                video_size_mb = num * 1024 if unit == "GB" else num
                            break
                    if video_size_mb is not None and video_size_mb > 500:
                        self.log(f"跳过超大视频 {bvid}，大小 {video_size_mb:.2f} MB 超过500 MB")
                        return False

                    # 将子目录中的mp4文件移到 target_dir
                    for fpath in glob.glob(os.path.join(target_dir, '**', '*.mp4'), recursive=True):
                        dpath = os.path.join(target_dir, os.path.basename(fpath))
                        if os.path.abspath(fpath) != os.path.abspath(dpath):
                            try:
                                shutil.move(fpath, dpath)
                                self.log(f"移动文件 {fpath} -> {dpath}")
                            except Exception as e:
                                self.log(f"移动文件失败: {e}")
                    
                    # 检查是否下载成功
                    mp4_files = glob.glob(os.path.join(target_dir, '*.mp4'))
                    if not mp4_files:
                        self.log(f"未找到下载的MP4文件，重试下载 (尝试 {attempt+1}/{self.max_retries})")
                        self.random_sleep(2, 5)
                        continue
                    
                    # 处理下载的视频
                    if self.process_video(target_dir, bvid):
                        self.downloaded_videos.add(bvid)
                        return True
                    else:
                        self.log(f"视频处理失败，重试下载 (尝试 {attempt+1}/{self.max_retries})")
                        self.random_sleep(2, 5)
                except subprocess.CalledProcessError as e:
                    self.log(f"下载视频失败: {e}")
                    continue
            
            self.log(f"达到最大重试次数，下载失败: {bvid}")
            return False
        
        def write_to_csv(self, video_data):
            """将视频信息写入CSV文件"""
            # 检查文件是否存在以确定是否需要写入表头
            file_exists = os.path.isfile(self.csv_file)
            
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as file:
                fieldnames = [
                    'bvid', 'aid', 'title', 'author', 'mid', 'duration', 
                    'view_count', 'danmaku', 'reply', 'favorite', 'coin', 
                    'share', 'like', 'upload_time', 'url', 'local_path'
                ]
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                    
                writer.writerow(video_data)
        
        def write_to_failed_downloads_csv(self, video_data, error_message):
            """将下载失败的视频信息写入CSV文件"""
            file_exists = os.path.isfile(self.failed_csv_file)
            
            with open(self.failed_csv_file, 'a', newline='', encoding='utf-8') as file:
                fieldnames = [
                    'bvid', 'title', 'author', 'mid', 'duration', 
                    'view_count', 'upload_time', 'url', 'error', 'timestamp'
                ]
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                # Construct data for the failed downloads CSV
                failed_entry = {
                    'bvid': video_data.get('bvid', ''),
                    'title': video_data.get('title', ''),
                    'author': video_data.get('author', ''),
                    'mid': video_data.get('mid', ''),
                    'duration': video_data.get('duration', 0),
                    'view_count': video_data.get('view_count', 0),
                    'upload_time': video_data.get('upload_time', ''),
                    'url': video_data.get('url', ''),
                    'error': error_message,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                writer.writerow(failed_entry)
        
        def search_videos_by_timeframe(self, time_range):
            """使用B站API直接按时间段搜索哈基米视频"""
            keyword = "哈基米"
            self.log(f"搜索关键词 '{keyword}' 在 {time_range['begin_date']} 至 {time_range['end_date']} 期间的视频")
            
            # 使用B站搜索API + UNIX时间戳
            url = "https://api.bilibili.com/x/web-interface/search/type"
            
            # 处理的视频列表
            all_videos = []
            
            # 页码
            page = 1
            max_pages = 20  # 增加最大页码数，确保获取所有结果
            has_more = True
            
            while page <= max_pages and has_more:
                self.log(f"获取第 {page} 页...")
                
                params = {
                    "search_type": "video",
                    "keyword": keyword,
                    "order": "pubdate",  # 按发布时间排序
                    "duration": "0",     # 不限时长
                    "tids": "0",         # 不限分区
                    "page": str(page),
                    "pubdate": "1",      # 启用时间筛选
                    "pubtime_begin_s": str(time_range['begin_timestamp']),
                    "pubtime_end_s": str(time_range['end_timestamp'])
                }
                
                # 发送请求
                try:
                    response = self.session.get(url, params=params, timeout=10)
                    self.log(f"响应状态码: {response.status_code}")
                    
                    if response.status_code == 200:
                        try:
                            json_data = response.json()
                            
                            if json_data.get("code") == 0 and json_data.get("data") and json_data["data"].get("result"):
                                results = json_data["data"]["result"]
                                self.log(f"找到 {len(results)} 个结果")
                                
                                # 检查是否有结果
                                if not results:
                                    self.log("没有更多结果，退出翻页")
                                    has_more = False
                                    break
                                
                                # 提取视频信息并添加到列表（先不筛选，收集所有数据）
                                for item in results:
                                    try:
                                        # 从HTML标签中提取纯文本
                                        title = item.get('title', '').replace('<em class="keyword">', '').replace('</em>', '')
                                        pubdate = item.get('pubdate', 0)
                                        pubdate_str = datetime.fromtimestamp(pubdate).strftime('%Y-%m-%d %H:%M:%S') if pubdate else 'N/A'
                                        
                                        # 解析播放量和时长
                                        try:
                                            view_count = int(item.get('play', 0))
                                        except (ValueError, TypeError):
                                            view_count = 0

                                        # 解析时长：支持字符串格式（如 'HH:MM:SS' 或 'MM:SS'）
                                        raw_duration = item.get('duration', 0)
                                        if isinstance(raw_duration, str) and ':' in raw_duration:
                                            duration = self.parse_duration(raw_duration)
                                        else:
                                            try:
                                                duration = int(raw_duration)
                                            except (ValueError, TypeError):
                                                duration = 0
                                        
                                        # 提取发布日期，用于文件名
                                        pub_date = datetime.fromtimestamp(pubdate).strftime('%Y%m%d') if pubdate else ''
                                        
                                        # 解析视频数据
                                        video_data = {
                                            'bvid': item.get('bvid', ''),
                                            'aid': item.get('aid', ''),
                                            'title': title,
                                            'author': item.get('author', ''),
                                            'mid': item.get('mid', ''),
                                            'duration': duration,
                                            'view_count': view_count,
                                            'danmaku': item.get('video_review', 0),
                                            'reply': item.get('review', 0),
                                            'favorite': item.get('favorites', 0),
                                            'coin': item.get('coins', 0),
                                            'share': item.get('share', 0),
                                            'like': item.get('like', 0),
                                            'upload_time': pubdate_str,
                                            'url': f"https://www.bilibili.com/video/{item.get('bvid', '')}",
                                            'local_path': ''  # 初始化，本地路径留空
                                        }
                                        
                                        # 添加到列表
                                        all_videos.append(video_data)
                                    except Exception as e:
                                        self.log(f"处理视频数据时出错: {e}")
                                
                                # 检查是否有下一页
                                # 判断是否有更多页 - B站API通常每页返回20条结果
                                has_more = len(results) >= 20
                                if not has_more:
                                    self.log(f"当前页结果数量为 {len(results)}，少于20条，可能没有下一页")
                                
                                # 检查是否已到达总数上限
                                if json_data.get("data") and json_data["data"].get("numResults"):
                                    total_results = int(json_data["data"]["numResults"])
                                    current_count = (page - 1) * 20 + len(results)
                                    if current_count >= total_results:
                                        self.log(f"已获取全部结果: {current_count}/{total_results}")
                                        has_more = False
                                
                            else:
                                self.log("API未返回结果")
                                has_more = False
                                break
                        except Exception as e:
                            self.log(f"解析响应数据时出错: {e}")
                            self.random_sleep(3, 7)
                            break
                    else:
                        self.log(f"请求失败，状态码: {response.status_code}")
                        self.random_sleep(5, 10)
                        # 连续失败不要立即退出，而是尝试其他页
                        if page > 1:  # 如果已经获取了一些结果，可以继续处理
                            has_more = False
                            break
                except Exception as e:
                    self.log(f"发送请求时出错: {e}")
                    self.random_sleep(5, 10)
                    if page > 1:  # 如果已经获取了一些结果，可以继续处理
                        has_more = False
                        break
                
                # 翻页
                page += 1
                self.random_sleep(2, 5)  # 适当延迟，避免请求过快
            
            # 搜索完成后，筛选视频
            self.log(f"搜索完成，共找到 {len(all_videos)} 个视频，开始筛选...")
            
            # 筛选播放量大于500，时长小于10分钟的视频
            filtered_videos = []
            for video in all_videos:
                view_count = video.get('view_count', 0)
                duration = video.get('duration', 0)
                
                if view_count >= self.min_view_count and duration <= self.max_duration:
                    filtered_videos.append(video)
                else:
                    reason = []
                    if view_count < self.min_view_count:
                        reason.append(f"播放量({view_count})低于{self.min_view_count}")
                    if duration > self.max_duration:
                        reason.append(f"时长({duration}秒)超过{self.max_duration}秒")
                    
                    self.log(f"筛选掉视频 {video.get('bvid', '')}: {video.get('title', '')} - 原因: {', '.join(reason)}")
            
            self.log(f"筛选后剩余 {len(filtered_videos)} 个视频")
            
            # 保存搜索结果到CSV
            result_dir = os.path.join(self.output_dir, "search_results")
            os.makedirs(result_dir, exist_ok=True)
            result_file = os.path.join(result_dir, f"{time_range['begin_date']}_{time_range['end_date']}_哈基米.csv")
            
            # 保存为CSV
            with open(result_file, 'w', newline='', encoding='utf-8') as f:
                # 使用与 video_info.csv 完全一致的列顺序
                fieldnames = [
                    'bvid', 'aid', 'title', 'author', 'mid', 'duration',
                    'view_count', 'danmaku', 'reply', 'favorite', 'coin',
                    'share', 'like', 'upload_time', 'url', 'local_path'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for video in filtered_videos:
                    # video_dict 已含 local_path 字段
                    writer.writerow(video)
            
            self.log(f"筛选后的视频列表已保存到 {result_file}")
            return filtered_videos
        
        def download_videos_by_timeframe(self, time_range):
            """下载指定时间范围内的视频"""
            begin = time_range['begin_date']
            end = time_range['end_date']
            self.log(f"开始下载 {begin} 至 {end} 期间的哈基米视频")
            
            # 尝试加载每周预先保存的搜索结果CSV
            result_file = os.path.join(self.output_dir, 'search_results', f"{begin}_{end}_哈基米.csv")
            videos = []
            if os.path.exists(result_file):
                self.log(f"加载每周搜索结果: {result_file}")
                try:
                    with open(result_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        videos = list(reader)
                    self.log(f"从周表加载了 {len(videos)} 条记录")
                    # 过滤缓存中的视频，确保符合播放量和时长要求
                    original_count = len(videos)
                    filtered_videos = []
                    for video in videos:
                        try:
                            vc = int(video.get('view_count', 0))
                        except:
                            vc = 0
                        try:
                            dur = int(video.get('duration', 0))
                        except:
                            dur = self.parse_duration(video.get('duration', '0'))
                        if vc >= self.min_view_count and dur <= self.max_duration:
                            filtered_videos.append(video)
                        else:
                            self.log(f"缓存中过滤掉视频 {video.get('bvid', '')} ({video.get('title', '')}): 时长{dur}s, 播放量{vc}")
                    videos = filtered_videos
                    self.log(f"缓存视频过滤后剩余 {len(videos)}/{original_count} 条")
                except Exception as e:
                    self.log(f"读取周表出错: {e}, 将重新搜索")
                    videos = self.search_videos_by_timeframe(time_range)
            else:
                self.log("周表不存在，开始搜索该时间段的视频...")
                videos = self.search_videos_by_timeframe(time_range)
            
            if not videos:
                self.log("未找到符合条件的视频，下载结束")
                return
            
            # 按播放量排序（降序）
            videos = sorted(videos, key=lambda x: int(x.get('view_count', 0)), reverse=True)
            
            # 检查video_info.csv是否存在，加载已下载视频
            self.log("重新加载已下载视频列表...")
            self.downloaded_videos = self._load_downloaded_videos()
            
            # 显示将要下载的视频
            self.log(f"找到 {len(videos)} 个视频，开始下载（按播放量排序）：")
            for i, video in enumerate(videos[:10]):  # 只显示前10个
                self.log(f"{i+1}. {video['title']} - 播放: {video['view_count']} - UP: {video['author']}")
            
            # 开始下载视频
            total_videos = len(videos)
            success_count = 0
            skip_count = 0
            fail_count = 0
            
            for i, video in enumerate(videos):
                bvid = video.get('bvid', '')
                if not bvid:
                    continue
                
                print_progress(i+1, total_videos, f"下载进度 ({begin}~{end})")
                self.log(f"处理视频 {i+1}/{len(videos)}: {video['title']}")
                # Debug: 打印原始 video 参数
                self.log(f"DEBUG video dict: {video}")
                
                # 检查是否已下载
                if self.is_video_downloaded(bvid):
                    self.log(f"视频 {bvid} 已经下载过，跳过")
                    skip_count += 1
                    continue
                
                # 获取视频信息
                try:
                    view_count = int(video.get('view_count', 0))
                except:
                    view_count = 0
                author = video.get('author', '')
                mid = video.get('mid', '')
                title = video.get('title', '')

                # 过滤超长视频（超过10分钟）
                try:
                    dur = int(video.get('duration', 0))
                except (ValueError, TypeError):
                    dur = self.parse_duration(video.get('duration', '0'))
                if dur > self.max_duration:
                    self.log(f"跳过超长视频 {video.get('bvid', '')}：时长 {dur} 秒")
                    skip_count += 1
                    continue

                # 根据播放量确定目标目录
                view_dir = self.get_target_directory(view_count)
                
                # 在播放量目录下创建作者目录
                author_dir = os.path.join(view_dir, f"{author}_{mid}")
                os.makedirs(author_dir, exist_ok=True)
                
                # 构建下载目标目录 (保持原样，不再创建额外子目录)
                target_dir = author_dir
                
                # 下载视频
                self.log(f"下载视频: {video['title']} 到 {target_dir}")
                download_success = self.download_video(bvid, mid, author, target_dir, title)
                
                if download_success:
                    # 准备视频数据以写入CSV
                    video_data = {
                        'bvid': bvid,
                        'aid': video.get('aid', ''),
                        'title': title,
                        'author': author,
                        'mid': mid,
                        'duration': video.get('duration', 0),
                        'view_count': view_count,
                        'danmaku': video.get('danmaku', 0),
                        'reply': video.get('reply', 0),
                        'favorite': video.get('favorite', 0),
                        'coin': video.get('coin', 0),
                        'share': video.get('share', 0),
                        'like': video.get('like', 0),
                        'upload_time': video.get('upload_time', ''),
                        'url': video.get('url', ''),
                        'local_path': target_dir
                    }
                    
                    # 写入CSV
                    self.write_to_csv(video_data)
                    success_count += 1
                    
                    # 清理非final的文件
                    self._clean_non_final_files(target_dir)
                else:
                    # 下载失败，记录到失败列表
                    self.log(f"下载失败: {bvid} - {title}")
                    fail_count += 1
                    error_message = f"下载失败，可能的原因：网络问题、视频被删除或设为私有"
                    self.write_to_failed_downloads_csv(video, error_message)
                
                # 视频间随机延迟
                if i < len(videos) - 1:  # 如果不是最后一个视频
                    self.random_sleep(3, 6)
            
            self.log(f"本周期视频下载完成：成功 {success_count}，跳过 {skip_count}，失败 {fail_count}")
            return
        
        def delete_downloaded_videos(self):
            """删除所有已下载的视频"""
            self.log("正在删除整个hachimi_videos目录...")
            
            # 直接删除整个输出目录
            try:
                if os.path.exists(self.output_dir):
                    import shutil
                    shutil.rmtree(self.output_dir)
                    self.log(f"已删除目录: {self.output_dir}")
                else:
                    self.log(f"输出目录 {self.output_dir} 不存在，无需删除")
                    
                # 重新创建输出目录
                os.makedirs(self.output_dir, exist_ok=True)
                self.log(f"已重新创建输出目录: {self.output_dir}")
                
                # 清空已下载视频集合
                self.downloaded_videos = set()
                
                # 重置CSV文件路径
                self.csv_file = os.path.join(self.output_dir, "video_info.csv")
                
                # 重新创建子目录
                self.create_directories()
                
                self.log("删除完成，准备重新下载")
            except Exception as e:
                self.log(f"删除目录失败: {e}")
        
        def generate_weekly_timeframes(self, start_date_str="2024-03-09", end_date_str="2025-06-06"):
            """生成从起始日期到结束日期的每7天的时间范围"""
            # 将字符串转换为日期对象
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            # 存储所有周的时间范围
            time_frames = []
            
            # 从起始日期开始
            current_date = start_date
            while current_date < end_date:
                # 计算当前周的结束日期（7天后）
                week_end_date = current_date + timedelta(days=6)
                
                # 如果结束日期超过了总的结束日期，使用总的结束日期
                if week_end_date > end_date:
                    week_end_date = end_date
                
                # 转换为时间戳
                begin_timestamp = int(time.mktime(current_date.timetuple()))
                end_timestamp = int(time.mktime(week_end_date.timetuple())) + 86399  # 加上一天减1秒
                
                # 添加到列表中
                time_frames.append({
                    'begin_date': current_date.strftime("%Y-%m-%d"),
                    'end_date': week_end_date.strftime("%Y-%m-%d"),
                    'begin_timestamp': begin_timestamp,
                    'end_timestamp': end_timestamp
                })
                
                # 移到下一周
                current_date = week_end_date + timedelta(days=1)
            
            return time_frames
        
        def batch_download_all_videos(self, start_date="2024-03-09", end_date="2025-06-06"):
            """批量下载从起始日期到结束日期的所有视频，按7天为一个时间段"""
            self.log(f"批量下载从 {start_date} 到 {end_date} 的所有哈基米视频")
            
            # 确保输出目录存在
            os.makedirs(self.output_dir, exist_ok=True)
            # 确保子目录存在
            self.create_directories()
            
            # 确保search_results目录存在
            search_results_dir = os.path.join(self.output_dir, "search_results")
            os.makedirs(search_results_dir, exist_ok=True)
            
            # 刷新已下载视频列表
            self.downloaded_videos = self._load_downloaded_videos()
            self.log(f"已加载 {len(self.downloaded_videos)} 个已下载视频")
            
            # 生成时间段列表
            time_frames = self.generate_weekly_timeframes(start_date, end_date)
            self.log(f"共生成 {len(time_frames)} 个时间段")
            
            # 创建总体进度文件
            progress_file = os.path.join(self.output_dir, "download_progress.txt")
            with open(progress_file, 'w', encoding='utf-8') as f:
                f.write(f"开始时间: {datetime.now()}\n")
                f.write(f"计划下载时间段: {start_date} - {end_date}\n")
                f.write(f"总时间段数: {len(time_frames)}\n\n")
            
            # 总体统计数据
            total_found = 0
            total_downloaded = 0
            total_skipped = 0
            total_failed = 0
            
            # 逐个时间段下载
            for i, time_frame in enumerate(time_frames):
                week_start = time_frame['begin_date']
                week_end = time_frame['end_date']
                
                self.log(f"===============================================")
                self.log(f"处理第 {i+1}/{len(time_frames)} 个时间段: {week_start} 到 {week_end}")
                print_progress(i+1, len(time_frames), "总体进度")
                
                # 更新进度文件
                with open(progress_file, 'a', encoding='utf-8') as f:
                    f.write(f"开始处理时间段 {i+1}/{len(time_frames)}: {week_start} - {week_end} 于 {datetime.now()}\n")
                
                try:
                    # 先检查该周的search_results是否已存在
                    result_file = os.path.join(search_results_dir, f"{week_start}_{week_end}_哈基米.csv")
                    videos_from_cache = []
                    
                    if os.path.exists(result_file):
                        self.log(f"发现缓存的搜索结果: {result_file}")
                        try:
                            # 尝试从缓存加载
                            with open(result_file, 'r', encoding='utf-8') as f:
                                reader = csv.DictReader(f)
                                videos_from_cache = list(reader)
                            
                            self.log(f"从缓存加载了 {len(videos_from_cache)} 个视频信息")
                            
                            # 验证是否需要重新筛选
                            needs_filtering = False
                            for video in videos_from_cache:
                                view_count = int(video.get('view_count', 0))
                                duration = int(video.get('duration', 0))
                                
                                if view_count < self.min_view_count or duration > self.max_duration:
                                    needs_filtering = True
                                    break
                            
                            if needs_filtering:
                                self.log("缓存的结果需要重新筛选，将重新搜索")
                                videos_from_cache = []
                        except Exception as e:
                            self.log(f"读取缓存文件出错: {e}")
                            videos_from_cache = []
                    
                    # 如果没有缓存或缓存无效，重新搜索
                    if not videos_from_cache:
                        self.log("开始搜索该时间段的视频...")
                        videos = self.search_videos_by_timeframe(time_frame)
                    else:
                        self.log("使用缓存的搜索结果")
                        videos = videos_from_cache
                    
                    # 记录找到的视频数量
                    videos_found = len(videos)
                    total_found += videos_found
                    
                    if not videos:
                        self.log(f"时间段 {week_start} - {week_end} 未找到符合条件的视频，跳过")
                        with open(progress_file, 'a', encoding='utf-8') as f:
                            f.write(f"  时间段 {week_start} - {week_end} 未找到视频\n")
                        continue
                    
                    self.log(f"开始下载 {videos_found} 个视频...")
                    
                    # 下载该时间段的视频
                    self.download_videos_by_timeframe(time_frame)
                    
                    # 统计下载结果
                    # 注意：这里的统计不太准确，因为download_videos_by_timeframe没有返回统计数据
                    # 我们在这里通过对比下载前后的downloaded_videos集合大小来估算
                    new_downloaded = self._load_downloaded_videos()
                    videos_added = len(new_downloaded) - len(self.downloaded_videos)
                    if videos_added > 0:
                        total_downloaded += videos_added
                        self.downloaded_videos = new_downloaded
                    
                    # 更新进度文件
                    with open(progress_file, 'a', encoding='utf-8') as f:
                        f.write(f"  完成时间段 {week_start} - {week_end}: 找到 {videos_found} 个视频\n")
                        
                except Exception as e:
                    self.log(f"处理时间段 {week_start} - {week_end} 时出错: {e}")
                    traceback.print_exc()
                    with open(progress_file, 'a', encoding='utf-8') as f:
                        f.write(f"  处理时间段 {week_start} - {week_end} 出错: {str(e)}\n")
                
                # 时间段之间的延迟，避免请求过快
                if i < len(time_frames) - 1:  # 如果不是最后一个时间段
                    delay = random.randint(5, 15)
                    self.log(f"等待 {delay} 秒后继续下一个时间段...")
                    time.sleep(delay)
            
            # 完成所有下载后的总结
            self.log(f"===============================================")
            self.log(f"所有时间段处理完成")
            self.log(f"总计找到视频: {total_found}")
            self.log(f"总计下载成功: {total_downloaded}")
            
            # 更新进度文件
            with open(progress_file, 'a', encoding='utf-8') as f:
                f.write(f"\n完成时间: {datetime.now()}\n")
                f.write(f"总计找到视频: {total_found}\n")
                f.write(f"总计下载成功: {total_downloaded}\n")
            
            return True
        
        async def process_weekly_videos(self):
            """异步版本的周处理函数(为了保持兼容性)"""
            # 实际上直接调用同步版本
            self.batch_download_all_videos()
            return True

    async def main():
        try:
            print("创建爬虫实例...")
            crawler = BilibiliCrawler()
            
            # 检查是否有命令行参数提供SESSDATA
            if len(sys.argv) > 1 and sys.argv[1].startswith("SESSDATA="):
                print("检测到命令行提供的SESSDATA，直接使用...")
                sessdata = sys.argv[1].split("=", 1)[1]
                if not sessdata.strip():
                    print("提供的SESSDATA为空，将使用正常登录流程")
                else:
                    # 更新当前会话的cookie
                    crawler.session.cookies.set('SESSDATA', sessdata, domain='.bilibili.com')
                    # 保存会话数据
                    crawler._save_session(sessdata)
                    
                    # 验证登录状态
                    if crawler._verify_login():
                        print("命令行提供的SESSDATA有效，登录成功")
                        crawler.is_logged_in = True
                    else:
                        print("提供的SESSDATA无效，将使用正常登录流程")
                        # 仅当验证失败时才调用登录
                        if not crawler.login():
                            print("登录失败，程序退出")
                            sys.exit(1)
            else:
                # 正常登录流程
                # 注意：只有在未登录状态才调用login
                if not crawler.is_logged_in and not crawler.login():
                    print("登录失败，程序退出")
                    sys.exit(1)
            
            # 检查是否需要清空所有文件
            if "--clear-all" in sys.argv:
                print("清空所有已下载的视频...")
                crawler.delete_downloaded_videos()
                print("清空完成，退出程序")
                sys.exit(0)
            
            # 检查是否指定了时间搜索模式
            if len(sys.argv) > 1 and sys.argv[1] == "--search-time":
                print("进入时间搜索模式...")
                if len(sys.argv) >= 4:
                    # 格式: py hachimi_crawler.py --search-time 2022-11-19 2022-11-26
                    begin_date = sys.argv[2]
                    end_date = sys.argv[3]
                    
                    # 解析时间
                    begin_timestamp = int(time.mktime(datetime.strptime(begin_date, '%Y-%m-%d').timetuple()))
                    end_timestamp = int(time.mktime(datetime.strptime(end_date, '%Y-%m-%d').timetuple())) + 86399  # 加上一天减1秒
                    
                    time_range = {
                        'begin_date': begin_date,
                        'end_date': end_date,
                        'begin_timestamp': begin_timestamp,
                        'end_timestamp': end_timestamp
                    }
                    
                    print(f"搜索时间范围: {begin_date} 到 {end_date}")
                    print(f"时间戳范围: {begin_timestamp} 到 {end_timestamp}")
                    
                    # 执行时间搜索并下载
                    crawler.download_videos_by_timeframe(time_range)
                else:
                    print("参数不足! 格式: py hachimi_crawler.py --search-time 开始日期 结束日期")
                    print("例如: py hachimi_crawler.py --search-time 2022-11-19 2022-11-26")
                
                sys.exit(0)
            
            # 测试特定时间段
            if len(sys.argv) > 1 and sys.argv[1] == "--test-time-range":
                print("测试特定时间段搜索功能...")
                # 测试2023年整年
                time_range = {
                    'begin_date': '2023-01-01',
                    'end_date': '2023-12-31',
                    'begin_timestamp': 1672502400,  # 2023-01-01
                    'end_timestamp': 1704038399    # 2023-12-31 23:59:59
                }
                
                result = await crawler.search_videos_by_time_range(time_range)
                print(f"2023年全年搜索结果: 找到 {len(result['videos'])} 个视频")
                
                for video in result['videos'][:5]:  # 打印前5个视频信息
                    print(f"视频: {video.get('title')} - {video.get('pub_time')}")
                
                sys.exit(0)
            
            # 如果没有参数，默认执行批量下载从2022-11-19到2025-06-02的所有视频
            print("无参数模式，开始批量下载...")
            crawler.batch_download_all_videos()
        except Exception as e:
            print(f"主程序出错: {e}")
            traceback.print_exc()

    def directly_save_sessdata():
        """直接保存提供的SESSDATA到文件"""
        try:
            print("\n" + "="*50)
            print("请手动提供SESSDATA：")
            print("1. 打开浏览器，访问bilibili.com并确保已登录")
            print("2. 按F12打开开发者工具，切换到'应用'或'Application'标签")
            print("3. 在左侧找到'Cookies'，然后找到bilibili.com")
            print("4. 在右侧找到名为'SESSDATA'的cookie，复制其值")
            print("5. 粘贴到下方并按回车")
            print("="*50)
            
            sessdata = input("SESSDATA: ").strip()
            
            if not sessdata:
                print("未提供SESSDATA，操作取消")
                return False
                
            session_file = "bili_session.pickle" # Use relative path
            
            session_data = {
                'cookies': {
                    'SESSDATA': sessdata
                },
                'timestamp': datetime.now().timestamp(),
                'user_agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            with open(session_file, 'wb') as f:
                pickle.dump(session_data, f)
            
            # 同时更新BBDown的cookie
            bbdown_cookie_dir = os.path.join(os.path.expanduser("~"), ".bbdown")
            cookie_file = os.path.join(bbdown_cookie_dir, "cookies.json")
            
            try:
                os.makedirs(bbdown_cookie_dir, exist_ok=True)
                with open(cookie_file, 'w', encoding='utf-8') as f:
                    json.dump({'SESSDATA': sessdata}, f)
                print(f"已更新BBDown的cookie文件：{cookie_file}")
            except Exception as e:
                print(f"更新BBDown cookie时出错: {e}")
            
            print(f"SESSDATA已保存到 {session_file}")
            
            # 验证会话有效性
            print("正在验证SESSDATA是否有效...")
            session = requests.Session()
            session.cookies.set('SESSDATA', sessdata, domain='.bilibili.com')
            
            try:
                test_url = "https://api.bilibili.com/x/web-interface/nav"
                response = session.get(test_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("code") == 0 and data.get("data", {}).get("isLogin"):
                        print(f"登录验证成功，用户名: {data.get('data', {}).get('uname')}")
                        return True
                    else:
                        print(f"登录验证失败，API返回: {data.get('message')}")
                else:
                    print(f"登录验证失败，状态码: {response.status_code}")
            except Exception as e:
                print(f"验证登录状态时出错: {e}")
                
            return False
        except Exception as e:
            print(f"保存SESSDATA时出错: {e}")
            return False

    if __name__ == "__main__":
        # 如果有--save-sessdata参数，则直接保存SESSDATA
        if len(sys.argv) > 1 and sys.argv[1] == "--save-sessdata":
            directly_save_sessdata()
            sys.exit(0)
        
        print("进入主程序...")
        try:
            asyncio.run(main())
            print("成功完成。")
        except Exception as e:
            print(f"致命错误: {e}")
            traceback.print_exc()
except Exception as e:
    print(f"导入或类定义过程中出现异常: {e}")
    traceback.print_exc()

print("脚本执行完毕。") 