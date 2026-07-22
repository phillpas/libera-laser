set(LIBERA_UNIT_TESTS
  test_core_buffer_estimator.cpp
  test_core_controller_cache.cpp
  test_core_controller_status.cpp
  test_core_content_source_switch.cpp
  test_core_frame_request.cpp
  test_core_frame_scheduler.cpp
  test_core_frame_advance.cpp
  test_core_projected_render_time.cpp
  test_core_queue_budget_floor.cpp
  test_core_single_controller_manager_base.cpp
  test_core_encoding.cpp
  test_core_byte_buffer.cpp
  test_core_frame_transition.cpp
  test_core_scanner_sync.cpp
  test_point_stream_framer.cpp
)

if(LIBERA_ENABLE_LASERCUBENET)
  list(APPEND LIBERA_UNIT_TESTS
    test_lasercubenet_buffer_target.cpp
    test_lasercubenet_lifecycle.cpp
    test_lasercubenet_point_rate.cpp
    test_lasercubenet_registration.cpp
    test_lasercubenet_status.cpp)
endif()

if(LIBERA_ENABLE_ETHERDREAM)
  list(APPEND LIBERA_UNIT_TESTS
    test_etherdream_status.cpp
    test_etherdream_buffer_target.cpp
    test_etherdream_buffer_projection.cpp
    test_etherdream_command.cpp)
endif()

if(LIBERA_ENABLE_AVB)
  list(APPEND LIBERA_UNIT_TESTS test_avb_backend_state.cpp)
endif()

if(LIBERA_ENABLE_HELIOS)
  list(APPEND LIBERA_UNIT_TESTS test_helios_timing.cpp)
endif()

foreach(test_source IN LISTS LIBERA_UNIT_TESTS)
  get_filename_component(test_name ${test_source} NAME_WE)
  add_executable(${test_name} ${test_source})
  target_link_libraries(${test_name} PRIVATE libera-core)
  libera_apply_warnings(${test_name})
  libera_apply_sanitizers(${test_name})
  add_test(NAME ${test_name} COMMAND ${test_name})
  set_tests_properties(${test_name} PROPERTIES LABELS "unit")
endforeach()

if(LIBERA_BUILD_INTEGRATION_TESTS AND LIBERA_ENABLE_ETHERDREAM)
  set(LIBERA_INTEGRATION_TESTS
    test_etherdream_begin_timeout_reconnect.cpp
    test_etherdream_firmware_probe.cpp
    test_etherdream_reconnect.cpp
    test_etherdream_stream_order.cpp)
  foreach(test_source IN LISTS LIBERA_INTEGRATION_TESTS)
    get_filename_component(test_name ${test_source} NAME_WE)
    add_executable(${test_name} ${test_source})
    target_link_libraries(${test_name} PRIVATE libera-core)
    libera_apply_warnings(${test_name})
    libera_apply_sanitizers(${test_name})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_tests_properties(${test_name} PROPERTIES LABELS "integration")
  endforeach()
endif()

if(LIBERA_BUILD_HARDWARE_TESTS)
  set(LIBERA_HARDWARE_TESTS)
  if(LIBERA_ENABLE_ETHERDREAM)
    list(APPEND LIBERA_HARDWARE_TESTS
      test_etherdream_packet_latency.cpp
      test_etherdream_protocol_probe.cpp
      test_etherdream_underflow_probe.cpp)
  endif()
  if(LIBERA_ENABLE_HELIOS)
    list(APPEND LIBERA_HARDWARE_TESTS test_helios_usb_stream.cpp)
  endif()
  foreach(test_source IN LISTS LIBERA_HARDWARE_TESTS)
    get_filename_component(test_name ${test_source} NAME_WE)
    add_executable(${test_name} ${test_source})
    target_link_libraries(${test_name} PRIVATE libera-core)
    libera_apply_warnings(${test_name})
    libera_apply_sanitizers(${test_name})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_tests_properties(${test_name} PROPERTIES LABELS "hardware")
  endforeach()
endif()
