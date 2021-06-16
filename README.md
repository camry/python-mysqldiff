# mysqldiff

> 注：已不在维护 Python 版本的 mysqldiff 工具，请前往 Golang 版本的 [go-mysqldiff](https://github.com/camry/go-mysqldiff) 工具。

Python 针对 MySQL 数据库表结构的差异 SQL 工具。

## 使用

```bash
# 查看帮助
./bin/mysqldiff --help
# 实例
./bin/mysqldiff --source user:password@host:port --db db1:db2
./bin/mysqldiff --source user:password@host:port --target user:password@host:port --db db1:db2
```

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
