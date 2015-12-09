#ifndef PROCMAN_EXEC_STRING_UTILS_HPP__
#define PROCMAN_EXEC_STRING_UTILS_HPP__

#include <map>
#include <string>
#include <vector>

#include "procman.hpp"

namespace procman {

/**
 * Do variable expansion on a command argument.  This searches the argument for
 * text of the form $VARNAME and ${VARNAME}.  For each discovered variable, it
 * then expands the variable using the environment. If a variable expansion
 * fails, then the corresponding text is left unchanged.
 */
std::string ExpandVariables(const std::string& input);

std::vector<std::string> SeparateArgs(const std::string& input);

std::vector<std::string> Split(const std::string& input,
    const std::string& delimeters,
    int max_items);

void Strfreev(char** vec);

}  // namespace procman

#endif  // PROCMAN_EXEC_STRING_UTILS_HPP__
