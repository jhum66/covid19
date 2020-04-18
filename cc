while true
do
    cd ..
    cd COVID-19-web-data
    git pull
    cd ..
    cd covid19
    python3 load_web.py
    sleep 14400
done
