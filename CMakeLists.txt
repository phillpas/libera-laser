cmake_minimum_required(VERSION 3.15)

project(libera-core LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SANITIZE "none" CACHE STRING "Sanitizer to use (none|address|thread)")
set_property(CACHE SANITIZE PROPERTY STRINGS none address thread)

option(LIBERA_ENABLE_LASERCUBENET "Enable the LaserCube network backend" ON)
option(LIBERA_ENABLE_ETHERDREAM "Enable the EtherDream network backend" OFF)
option(LIBERA_ENABLE_HELIOS "Enable the Helios USB backend" OFF)
option(LIBERA_ENABLE_IDN "Enable the Helios IDN backend" OFF)
option(LIBERA_ENABLE_LASERCUBEUSB "Enable the LaserCube USB backend" OFF)
option(LIBERA_ENABLE_AVB "Enable the AVB audio backend" OFF)
option(LIBERA_ENABLE_PLUGINS "Enable runtime plugin loading" OFF)
option(LIBERA_BUILD_TESTS "Build libera-core tests" ${BUILD_TESTING})
option(LIBERA_BUILD_INTEGRATION_TESTS "Build integration tests (loopback/network)" OFF)
option(LIBERA_BUILD_HARDWARE_TESTS "Build hardware tests (requires controller hardware)" OFF)
option(LIBERA_BUILD_EXAMPLES "Build example applications" OFF)
option(LIBERA_BUILD_LASER_TOOL "Build the GUI laser-tool example" OFF)

function(libera_apply_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive- /FS)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Sanitizers.cmake)

set(LIBERA_CORE_SOURCES
  src/core/BufferEstimator.cpp
  src/core/ByteBuffer.cpp
  src/core/FrameScheduler.cpp
  src/core/LaserController.cpp
  src/core/LaserControllerStreaming.cpp
  src/core/PointStreamFramer.cpp
  src/core/System.cpp
  src/log/Log.cpp
  src/net/NetService.cpp
)

if(LIBERA_ENABLE_LASERCUBENET)
  list(APPEND LIBERA_CORE_SOURCES
    src/lasercubenet/LaserCubeNetController.cpp
    src/lasercubenet/LaserCubeNetControllerInfo.cpp
    src/lasercubenet/LaserCubeNetManager.cpp
    src/lasercubenet/LaserCubeNetStatus.cpp
  )
endif()

if(LIBERA_ENABLE_ETHERDREAM)
  list(APPEND LIBERA_CORE_SOURCES
    src/EtherDream/EtherDreamCommand.cpp
    src/EtherDream/EtherDreamController.cpp
    src/EtherDream/EtherDreamManager.cpp
    src/EtherDream/EtherDreamResponse.cpp
  )
endif()

if(LIBERA_ENABLE_AVB)
  list(APPEND LIBERA_CORE_SOURCES
    src/avb/AvbBackendState.cpp
    src/avb/AvbController.cpp
    src/avb/AvbDeviceRuntime.cpp
    src/avb/AvbManager.cpp
  )
  if(APPLE)
    list(APPEND LIBERA_CORE_SOURCES src/avb/CoreAudioHost.cpp)
  endif()
endif()

set(LIBERA_NEEDS_HELIOS_SDK OFF)
if(LIBERA_ENABLE_HELIOS OR LIBERA_ENABLE_IDN)
  set(LIBERA_NEEDS_HELIOS_SDK ON)
endif()

if(LIBERA_ENABLE_HELIOS)
  list(APPEND LIBERA_CORE_SOURCES
    src/helios/HeliosController.cpp
    src/helios/HeliosManager.cpp
    src/usb/LibusbSafe.cpp
  )
endif()

if(LIBERA_ENABLE_IDN)
  list(APPEND LIBERA_CORE_SOURCES
    src/idn/IdnController.cpp
    src/idn/IdnManager.cpp
  )
endif()

if(LIBERA_ENABLE_LASERCUBEUSB)
  list(APPEND LIBERA_CORE_SOURCES
    src/lasercubeusb/LaserCubeUsbController.cpp
    src/lasercubeusb/LaserCubeUsbManager.cpp
    src/usb/LibusbSafe.cpp
  )
endif()

if(LIBERA_ENABLE_PLUGINS)
  list(APPEND LIBERA_CORE_SOURCES
    src/plugin/PluginController.cpp
    src/plugin/PluginManager.cpp
    src/plugin/PluginRegistry.cpp
  )
endif()

add_library(libera-core ${LIBERA_CORE_SOURCES})

target_include_directories(libera-core
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libs/asio/include>
)

foreach(backend IN ITEMS LASERCUBENET ETHERDREAM HELIOS IDN LASERCUBEUSB AVB PLUGINS)
  if(LIBERA_ENABLE_${backend})
    target_compile_definitions(libera-core PUBLIC LIBERA_ENABLE_${backend}=1)
  else()
    target_compile_definitions(libera-core PUBLIC LIBERA_ENABLE_${backend}=0)
  endif()
endforeach()

libera_apply_warnings(libera-core)
libera_apply_sanitizers(libera-core)

if(LIBERA_NEEDS_HELIOS_SDK OR LIBERA_ENABLE_LASERCUBEUSB)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Libusb.cmake)
  option(LIBERA_USE_BUNDLED_LIBUSB "Use bundled libusb from libs/helios_dac" OFF)
  set(LIBERA_BUNDLED_LIBUSB_DIR
      "${CMAKE_CURRENT_SOURCE_DIR}/libs/helios_dac/sdk/cpp"
      CACHE PATH "Path to bundled libusb headers/binaries")
  libera_configure_libusb()
  target_include_directories(libera-core PRIVATE "${LIBERA_BUNDLED_LIBUSB_DIR}")
  target_link_libraries(libera-core PUBLIC libusb::libusb)
endif()

if(LIBERA_NEEDS_HELIOS_SDK)
  if(NOT EXISTS "${LIBERA_BUNDLED_LIBUSB_DIR}/HeliosDac.cpp")
    message(FATAL_ERROR "Helios/IDN requires the libs/helios_dac submodule")
  endif()
  set(HELIOS_SDK_SOURCES "${LIBERA_BUNDLED_LIBUSB_DIR}/HeliosDac.cpp")
  if(LIBERA_ENABLE_IDN)
    list(APPEND HELIOS_SDK_SOURCES
      "${LIBERA_BUNDLED_LIBUSB_DIR}/idn/idn.cpp"
      "${LIBERA_BUNDLED_LIBUSB_DIR}/idn/idnServerList.cpp")
    if(WIN32)
      list(APPEND HELIOS_SDK_SOURCES "${LIBERA_BUNDLED_LIBUSB_DIR}/idn/plt-windows.cpp")
    else()
      list(APPEND HELIOS_SDK_SOURCES "${LIBERA_BUNDLED_LIBUSB_DIR}/idn/plt-posix.cpp")
    endif()
  endif()
  if(EXISTS "${LIBERA_BUNDLED_LIBUSB_DIR}/HeliosLog.cpp")
    list(APPEND HELIOS_SDK_SOURCES "${LIBERA_BUNDLED_LIBUSB_DIR}/HeliosLog.cpp")
  endif()
  add_library(helios-sdk OBJECT ${HELIOS_SDK_SOURCES})
  target_include_directories(helios-sdk PRIVATE "${LIBERA_BUNDLED_LIBUSB_DIR}")
  if(LIBERA_ENABLE_IDN)
    target_compile_definitions(helios-sdk PRIVATE HELIOS_ENABLE_IDN_DISCOVERY=1)
  endif()
  if(MSVC)
    target_compile_options(helios-sdk PRIVATE /W0)
  else()
    target_compile_options(helios-sdk PRIVATE -w)
  endif()
  target_sources(libera-core PRIVATE $<TARGET_OBJECTS:helios-sdk>)
endif()

if(LIBERA_ENABLE_PLUGINS AND NOT WIN32)
  target_link_libraries(libera-core PRIVATE ${CMAKE_DL_LIBS})
endif()

if(WIN32)
  target_link_libraries(libera-core PUBLIC ws2_32)
  target_compile_definitions(libera-core PUBLIC _WIN32_WINNT=0x0A00)
endif()

if(LIBERA_ENABLE_AVB AND APPLE)
  find_library(CORE_AUDIO_FRAMEWORK CoreAudio REQUIRED)
  find_library(CORE_FOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
  find_library(AUDIO_TOOLBOX_FRAMEWORK AudioToolbox REQUIRED)
  target_link_libraries(libera-core
    PUBLIC
      ${CORE_AUDIO_FRAMEWORK}
      ${CORE_FOUNDATION_FRAMEWORK}
      ${AUDIO_TOOLBOX_FRAMEWORK})
endif()

if(BUILD_TESTING AND LIBERA_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(LIBERA_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
