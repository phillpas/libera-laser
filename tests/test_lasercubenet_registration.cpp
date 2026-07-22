#include "libera/System.hpp"
#include "libera/lasercubenet/LaserCubeNetControllerInfo.hpp"
#include "libera/lasercubenet/LaserCubeNetManager.hpp"

#include <algorithm>

int main() {
    const auto managers = libera::System::availableControllerManagers();
    const auto found = std::find_if(
        managers.begin(), managers.end(), [](const libera::core::ControllerManagerInfo& info) {
            return info.type ==
                   libera::lasercubenet::LaserCubeNetControllerInfo::controllerType();
        });
    return found == managers.end() ? 1 : 0;
}
