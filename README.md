# watchit

pkill -f "gunicorn.*server:app"
conda activate solilo
cd ~/watchit
nohup gunicorn -w 1 -b 127.0.0.1:5001 server:app > watchit.log 2>&1 &
