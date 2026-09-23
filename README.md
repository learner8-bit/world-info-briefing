# World info 云端推送

这是由本地 World info 配置导出的DeepSeek AI 简报。Webhook 和 AI Key 只应保存为 GitHub Actions Secret，不要提交 .env。

config/initial-state.json 只包含已处理时段、条数和已推送链接，用于首次云端去重；完整简报正文和密钥均未导出。后续运行用 GitHub Actions Cache 延续状态。
