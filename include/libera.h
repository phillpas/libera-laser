#pragma once

// Default public umbrella include:
// - pulls in the core API
// - registers all built-in controller managers via their static registrars
//
// For selective registration, include `libera/System.hpp`
// and only the specific `*Manager.hpp` headers you want.

#include "libera/System.hpp"
#if LIBERA_ENABLE_AVB
#include "libera/avb/AvbManager.hpp"
#endif
#include "libera/core/LaserController.hpp"
#include "libera/core/LaserPoint.hpp"
#include "libera/log/Log.hpp"

#if LIBERA_ENABLE_ETHERDREAM
#include "libera/etherdream/EtherDreamManager.hpp"
#endif
#if LIBERA_ENABLE_HELIOS
#include "libera/helios/HeliosManager.hpp"
#endif
#if LIBERA_ENABLE_IDN
#include "libera/idn/IdnManager.hpp"
#endif
#if LIBERA_ENABLE_LASERCUBENET
#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#endif
#if LIBERA_ENABLE_LASERCUBEUSB
#include "libera/lasercubeusb/LaserCubeUsbManager.hpp"
#endif
