#ifndef GET_NOW_HPP
#define GET_NOW_HPP
#include <chrono>
namespace
{
[[nodiscard]] std::chrono::microseconds getNow() 
{
     auto now 
        = std::chrono::duration_cast<std::chrono::microseconds>
          ((std::chrono::high_resolution_clock::now()).time_since_epoch());
     return now;
}
}
#endif
