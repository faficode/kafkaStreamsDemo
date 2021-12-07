docker run -it --name $1 --rm --network=host edenhill/kcat:1.7.0 -b localhost:9092 -C -o end -t $1 -q -K: -s value=i
