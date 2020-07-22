# mysqldiff

针对 MySQL 数据库表结构的差异 SQL 工具。

## 安装

```bash
pip install pyinstaller
pip install click
pip install mysql-connector-python
```

## 打包

```bash
pyinstaller -F mysqldiff.py
```

## 使用

```bash
# 查看帮助
./bin/mysqldiff --help
# 实例
./bin/mysqldiff --source user:password@host:port --db db1:db2
./bin/mysqldiff --source user:password@host:port --target user:password@host:port --db db1:db2
```
