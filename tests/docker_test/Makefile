NAME =
TAG = df_dup.DFDup
NS = disdat


all: build

build:
	docker build -t $(NS):$(TAG) .

push: build
	docker push $(NS)/$(NAME)-dev
	docker push $(NS)/$(NAME)-dev:$(TAG)
	docker push $(NS)/$(NAME)-deploy
	docker push $(NS)/$(NAME)-deploy:$(TAG)

shell:
	docker run  --rm disdat:df_dup.DFDup \
	--input-bundle input.bundle --output-bundle output.bundle \
	--branch mybranch \
	--remote s3://somebucket/somekeys