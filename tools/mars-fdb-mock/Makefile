IMAGE ?= eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1

.PHONY: smoke seed-requirements

smoke:
	./scripts/smoke.sh $(IMAGE)

seed-requirements:
	python3 -m pip install -r seed/requirements.txt
