include $(HOME)/setup/include/Makefile

# schemas stuff
schemas: src/autogen/schemas/YamlFile.tagged.schema.json

# interfaces stuff
interfaces: src/autogen/interfaces/YamlFile.ts

# schema for yaml database file
src/autogen/schemas/YamlFile.tagged.schema.json: templates/schemas/YamlFile.tagged.schema.jsonnet
	$(call render_jsonnet, $<, $@)

# interface for yaml database file
src/autogen/interfaces/YamlFile.ts: src/autogen/schemas/YamlFile.tagged.schema.json
	$(call generate_ts_interface, $<, $@)

