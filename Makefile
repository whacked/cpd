include $(HOME)/setup/include/Makefile

# schemas stuff
schemas: src/autogen/schemas/YamlFile.tagged.schema.json

# schema for yaml database file
src/autogen/schemas/YamlFile.tagged.schema.json: templates/schemas/YamlFile.tagged.schema.jsonnet
	$(call render_jsonnet, $<, $@)

# compile js output for external consumption
dist:
	tsc

.PHONY: dist
