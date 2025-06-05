# 哈基米视频爬虫工具

这是一个用于搜索和下载B站上"哈基米"相关视频的工具。该工具会自动搜索B站，筛选出时长少于10分钟的视频，并使用BBDown下载，然后通过ffmpeg进行格式转换。

## 功能特点

- 搜索B站上关键词为"哈基米"的视频
- 自动筛选时长少于10分钟的视频
- 按播放量对视频进行分类存储：
  - 100万以上播放量 (1M+)
  - 50万-100万播放量 (500K-1M)
  - 10万-50万播放量 (100K-500K)
  - 1万-10万播放量 (10K-100K)
  - 1万以下播放量 (Below_10K)
- 在每个播放量分类下，再按UP主分类存储
- 使用BBDown工具下载视频
- 使用ffmpeg对视频进行处理
- 记录视频元数据到CSV文件中
- 实现了反爬虫检测机制，包括随机延迟、用户代理轮换和请求重试

## 使用说明

### 环境要求

- Python 3.7+
- 依赖库：asyncio, json, os, csv, requests, subprocess, glob, random, time
- BBDown (`BBDown.exe`, 需与脚本在同一目录或在系统PATH中)
- ffmpeg (`ffmpeg-7.1.1-full_build/bin/ffmpeg.exe`, 相对于脚本的路径)

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行爬虫

```bash
python hachimi_crawler.py
```

## 输出结构

- 所有视频存储在 `hachimi_videos/` 目录下 (相对于脚本运行位置)
- 视频按播放量分类到不同的子目录中
- 每个播放量目录下，视频按UP主分类到不同的子目录
- 视频元数据记录在 `hachimi_videos/video_info.csv` 中 (相对于脚本运行位置)

## CSV文件字段说明

视频信息CSV文件包含以下字段：

- `bvid`: 视频的BV号
- `aid`: 视频的AV号
- `title`: 视频标题
- `author`: UP主名称
- `mid`: UP主ID
- `duration`: 视频时长（秒）
- `view_count`: 播放量
- `danmaku`: 弹幕数
- `reply`: 评论数
- `favorite`: 收藏数
- `coin`: 投币数
- `share`: 分享数
- `like`: 点赞数
- `upload_time`: 上传时间
- `url`: 视频URL
- `local_path`: 本地存储路径

## 注意事项

- 请合理控制爬取频率，避免对B站服务器造成过大负担
- 本工具仅供学习和研究目的使用，不得用于任何商业用途
- 下载的视频版权归原作者所有，请尊重版权
- 如遇到反爬虫检测，脚本会自动延长请求间隔并重试

## 自定义配置

如需修改配置，请编辑脚本中的以下参数：

- `self.output_dir`: 视频输出目录 (默认为相对路径 `hachimi_videos`)
- `self.max_duration`: 最大视频时长（秒）
- `self.search_keyword`: 搜索关键词
- `self.max_retries`: 最大重试次数

## 故障排除

如果遇到以下问题，请尝试解决方案：

1. **无法连接到B站API**：
   - 检查网络连接
   - 尝试修改请求头和延迟时间

2. **BBDown下载失败**：
   - 确保BBDown路径正确
   - 检查BBDown版本是否最新
   - 查看BBDown日志获取详细错误信息

3. **ffmpeg处理失败**：
   - 确保ffmpeg路径正确
   - 检查ffmpeg版本是否兼容 