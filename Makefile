default_target: usage
.PHONY : default_target upload

# VERSION := $(shell $(PYTHON) setup.py --version)

usage:
	@echo "The pc Makefile"
	@echo ""
	@echo "Usage : make <command> "
	@echo ""
	@echo "  build                 - builds the jar file and the \`pc\` command"
	@echo "  clean                 - cleans temp files"
	@echo ""

clean:
	mvn clean

build: 
	mvn clean package install
	cat scripts/main.sh target/pc.jar > pc
	chmod +x pc
