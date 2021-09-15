.PHONY: test
test: test_setup test_run test_clean

.PHONY: test_setup
test_setup:
	@echo "building the environment.."
	docker-compose -f tests/docker-compose.yml up --build -d --remove-orphans
	@sleep 5
	./tests/init.sh
	@echo "environment build is done."

.PHONY: test_run
test_run: export AWS_ACCESS_KEY_ID=aws-access-key-id
test_run: export AWS_SECRET_ACCESS_KEY=aws-secret-access-key
test_run:
	@echo "started run the all tests."
	go test -v -count 1 -p 1 -timeout 120s ./...
	@echo "all tests were completed."

.PHONY: test_clean
test_clean:
	@echo "cleaning the environment.."
	docker-compose -f tests/docker-compose.yml down
	@echo "environment cleaned up."
