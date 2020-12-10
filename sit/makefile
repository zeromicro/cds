PROJECT="CDS"

.PHONY : docker_build docker_run docker_build_run docker_infrastructrue_up docker_infrastructrue_down sit logo

logo: 
	@cat ./logo

docker_build: 
	@echo "================= build ======================"
	docker build -t cds ..

docker_run: 
	@echo "================= run ==============================="
	@docker-compose -f dockercompose/app.yml  up -d

docker_build_run: docker_build docker_run

docker_infrastructrue_up:
	@echo "============ infrastructure ========================="
	@docker-compose -f dockercompose/infrastructure.yml  up -d 
	
docker_infrastructrue_down:
	docker-compose -f dockercompose/infrastructure.yml  down

sit: docker_build_run docker_infrastructrue_up 

sit_down: docker_infrastructrue_down 
	docker-compose -f dockercompose/app.yml  down 
app_down:
	docker-compose -f dockercompose/app.yml  down  

up:  logo docker_build
	@echo "==================== launch ========================="
	@docker-compose -f dockercompose/infrastructure.yml  up -d
	@docker-compose -f dockercompose/app.yml up -d
	@cat ./info

down: 
	@docker-compose -f dockercompose/app.yml   -f dockercompose/infrastructure.yml down 