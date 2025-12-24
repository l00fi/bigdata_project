import os
import urllib.request

# Папка, куда кладем JAR-файлы (должна совпадать с volumes в docker-compose)
JARS_DIR = "spark/jars"

# Список необходимых библиотек (версии должны быть совместимы со Spark 3.5.1 / Hadoop 3)
JARS = {
    "hadoop-aws-3.3.4.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.517.jar": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.517.jar",
    "postgresql-42.7.2.jar": "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar"
}

def download_jars():
    if not os.path.exists(JARS_DIR):
        os.makedirs(JARS_DIR)
        print(f"Created directory: {JARS_DIR}")

    print(f"Downloading drivers to {JARS_DIR}...")

    for jar_name, url in JARS.items():
        file_path = os.path.join(JARS_DIR, jar_name)
        if os.path.exists(file_path):
            print(f"[SKIP] {jar_name} already exists.")
        else:
            print(f"[DOWNLOADING] {jar_name}...")
            try:
                urllib.request.urlretrieve(url, file_path)
                print(f"[OK] {jar_name} downloaded.")
            except Exception as e:
                print(f"[ERROR] Failed to download {jar_name}: {e}")

if __name__ == "__main__":
    download_jars()