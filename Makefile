build:
	mkdir -p ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -x \*libs\* -r ../dist/jobs.zip .