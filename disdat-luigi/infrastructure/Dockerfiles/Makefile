# Taken with gratitude from
# http://stackoverflow.com/questions/9249757/recursive-make-in-subdirectories
SHELL=/bin/bash

all:
	@for a in $$(ls); do \
        if [ -d $$a ]; then \
            echo "processing folder $$a"; \
            $(MAKE) -C $$a; \
        fi; \
	done;
	@echo "Done!"
