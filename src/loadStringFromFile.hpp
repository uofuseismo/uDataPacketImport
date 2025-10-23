#ifndef LOAD_STRING_FROM_FILE_HPP
#define LOAD_STRING_FROM_FILE_HPP
#include <fstream>
#include <filesystem>
#include <string>
#include <sstream>
namespace
{

[[nodiscard]] std::string
loadStringFromFile(const std::filesystem::path &path)
{
    std::string result;
    if (!std::filesystem::exists(path)){return result;}
    std::ifstream file(path);
    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open " + path.string());
    }   
    std::stringstream sstr;
    sstr << file.rdbuf();
    file.close(); 
    result = sstr.str();
    return result;
}

}
#endif
