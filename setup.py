from setuptools import setup, find_packages

# 读取 requirements.txt
with open("requirements.txt", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="autotrade",
    version="0.1.0",
    author="zhangbuzheng",
    description="A quantitative trading framework",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.8",
)
