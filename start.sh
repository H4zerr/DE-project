set -e

mkdir -p data

echo "Идёт скачивание файлов в папку ./data"

mkdir -p data/diploma_main_dataset
mkdir -p data/diploma_extra_dataset

curl -l "https://www.dropbox.com/scl/fi/cap4wucy6krldltfjqpx4/ga_hits.parquet.gz?rlkey=g6b9fc694o0seap0a17cabr60&st=3sk027ec&dl=1" -o data/diploma_main_dataset/ga_hits.parquet.gz
curl -l "https://www.dropbox.com/scl/fi/r9ogivk1iegbbighttx3j/ga_session.parquet.gz?rlkey=u3n8955elw5rff86uji8aixkl&st=96xb29hh&dl=1" -o data/diploma_main_dataset/ga_session.parquet.gz
curl -l "https://www.dropbox.com/scl/fi/eixpeyyqfkhd4ppzhe8i6/ga_hits_new_2022-01-01.json?rlkey=c2ffqu4qquco2lz0bzd93ijxs&st=tahdtrtz&dl=1" -o data/diploma_extra_dataset/ga_hits_new_2022-01-01.json
curl -l "https://www.dropbox.com/scl/fi/s2q0tqs3sfs4r8eswu1nj/ga_hits_new_2022-01-02.json?rlkey=grtxn6p3e8d36d6886evuc7dn&st=tl98m166&dl=1" -o data/diploma_extra_dataset/ga_hits_new_2022-01-02.json
curl -l "https://www.dropbox.com/scl/fi/g3a56zqimkbgg4x2mp9sg/ga_hits_new_2022-01-03.json?rlkey=qsqt1ef4r8ksmwcny6aixvtmc&st=twqngrsb&dl=1" -o data/diploma_extra_dataset/ga_hits_new_2022-01-03.json
curl -l "https://www.dropbox.com/scl/fi/x06mil0o7ocjk7zc97hul/ga_hits_new_2022-01-04.json?rlkey=q7gj3ffnuuc0utki823zj6qjz&st=avs28omo&dl=1" -o data/diploma_extra_dataset/ga_hits_new_2022-01-04.json
curl -l "https://www.dropbox.com/scl/fi/d0ap7l9e8v0wha8v5x9z1/ga_hits_new_2022-01-05.json?rlkey=3eozxwhk2ok7jfuepfvckox9w&st=58h98s8h&dl=1" -o data/diploma_extra_dataset/ga_hits_new_2022-01-05.json
curl -l "https://www.dropbox.com/scl/fi/nwfl9b8jvbwjwg9u5p5h6/ga_sessions_new_2022-01-01.json?rlkey=aiwl6n79x0cow19dxna5fijbd&st=bwgsb32o&dl=1" -o data/diploma_extra_dataset/ga_sessions_new_2022-01-01.json
curl -l "https://www.dropbox.com/scl/fi/dqs57l426890u145vf8uu/ga_sessions_new_2022-01-02.json?rlkey=0wgy0vgdyydi5wneaaebidwz9&st=jyhbcr8z&dl=1" -o data/diploma_extra_dataset/ga_sessions_new_2022-01-02.json
curl -l "https://www.dropbox.com/scl/fi/4qqh6vtcvs0b34w7g1ctz/ga_sessions_new_2022-01-03.json?rlkey=gtu7ykbv8am3hquz9xjr88dhd&st=554aiw7k&dl=1" -o data/diploma_extra_dataset/ga_sessions_new_2022-01-03.json
curl -l "https://www.dropbox.com/scl/fi/qdi64zwmu53981p7iw9wq/ga_sessions_new_2022-01-04.json?rlkey=t637h14on2clk7uke1zacygxb&st=stkaic11&dl=1" -o data/diploma_extra_dataset/ga_sessions_new_2022-01-04.json
curl -l "https://www.dropbox.com/scl/fi/h745lndi4jb5csnljg2p6/ga_sessions_new_2022-01-05.json?rlkey=lr746hb33c2xr50ewsogmpmuc&st=dkq669a8&dl=1" -o data/diploma_extra_dataset/ga_sessions_new_2022-01-05.json

mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up