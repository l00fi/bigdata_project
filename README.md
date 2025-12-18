# Список сервисов

1. prefect (http://localhost:9090) - дашборд для запуска пайплайнов;
2. spark-master (http://localhost:8080) - мониторинг кластера;
3. minio (http://localhost:9001) - дашборд хранилища (озеро данных) сырых данных;
4. grafana (http://localhost:3000) - дашборд для аналититки по данным. 

Данные postgres для grafana:
- URL: postgres:5432
- Database name: project_db
- Username: user
- Password: password
- SSL module: disabled

В случае если будет жаловаться на занятые порты:
- Запуск CMD от имени Администратора и запуск команд ```net stop winnat```, ```net start winnat```.

Дашборд из grafana/dashboards импортируем ручками
