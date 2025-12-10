# Список сервисов

1. prefect (http://localhost:9090) - дашборд для запуска пайплайнов;
2. spark-master (http://localhost:8080) - мониторинг кластера;
3. minio (http://localhost:9001) - дашборд хранилища (озеро данных) сырых данных;
4. grafana (http://localhost:3000) - лашборд для аналититки по данным. 

Данные postgres для grafana:
- URL: postgres:5432
- Database name: project_db
- Username: user
- Password: password
- SSL module: disabled
