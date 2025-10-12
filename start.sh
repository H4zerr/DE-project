mkdir data
mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up