set(FLATBUFFERS_SCHEMA ${PROJECT_SOURCE_DIR}/grpc/elliptics.fbs)

get_filename_component(BASENAME ${FLATBUFFERS_SCHEMA} NAME_WE)
set(IDL_COMPILED_FILES
    ${PROJECT_BINARY_DIR}/${BASENAME}_generated.h
    ${PROJECT_BINARY_DIR}/${BASENAME}.grpc.fb.h
    ${PROJECT_BINARY_DIR}/${BASENAME}.grpc.fb.cc
    )
set_source_files_properties(${IDL_COMPILED_FILES}
                            PROPERTIES GENERATED TRUE)

add_custom_command(
    OUTPUT ${IDL_COMPILED_FILES}
    DEPENDS ${FLATBUFFERS_SCHEMA}
    COMMAND ${PROJECT_BINARY_DIR}/foreign/flatbuffers/flatc --cpp --scoped-enums
        --grpc -o ${PROJECT_BINARY_DIR} --schema ${FLATBUFFERS_SCHEMA}
    COMMENT "Generate code for ${FLATBUFFERS_SCHEMA}"
    )
add_custom_target(generate-from-schema ALL DEPENDS flatc ${IDL_COMPILED_FILES})
