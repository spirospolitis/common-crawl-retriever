build:
	rm -rf ./dist
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py __pycache__/**\* -r ../dist/common_crawl_retriever.zip .