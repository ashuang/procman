#include "exec_string_utils.hpp"

#include <sstream>

#include <glib.h>

namespace procman {

std::vector<std::string> Split(const std::string& input,
    const std::string& delimeters,
    int max_items) {
  std::vector<std::string> result;

  int tok_begin = 0;
  int tok_end = 0;
  while (tok_begin < input.size()) {
    if (result.size() == max_items - 1) {
      result.emplace_back(&input[tok_begin]);
      return result;
    }

    for (tok_end = tok_begin;
        tok_end < input.size() &&
        !strchr(delimeters.c_str(), input[tok_end]);
        ++tok_end) {}

    result.emplace_back(&input[tok_begin], tok_end - tok_begin);

    tok_begin = tok_end + 1;
  }

  return result;
}

void Strfreev(char** vec) {
  for (char** ptr = vec; *ptr; ++ptr) {
    free(*ptr);
  }
}

class VariableExpander {
  public:
    VariableExpander(const std::string& input,
        const StringStringMap& vars) :
      input_(input),
      pos_(0),
      variables_(&vars) {
      Process();
    }

    const std::string Result() const { return output_.str(); }

  private:
    void Process() {
      while(EatToken()) {
        const char c = cur_tok_;
        if('\\' == c) {
          if(EatToken()) {
            output_.put(c);
          } else {
            output_.put('\\');
          }
          continue;
        }
        // variable?
        if('$' == c) {
          ParseVariable();
        } else {
          output_.put(c);
        }
      }
    }

    bool HasToken() {
      return pos_ < input_.size();
    }

    char PeekToken() {
      return HasToken() ? input_[pos_] : 0;
    }

    bool EatToken() {
      if(HasToken()) {
        cur_tok_ = input_[pos_];
        pos_++;
        return true;
      } else {
        cur_tok_ = 0;
        return false;
      }
    }

    bool ParseVariable() {
      int start = pos_;
      if(!HasToken()) {
        output_.put('$');
        return false;
      }
      int has_braces = PeekToken() == '{';
      if(has_braces) {
        EatToken();
      }
      int varname_start = pos_;
      int varname_len = 0;
      while(HasToken() &&
          IsValidVariableCharacter(PeekToken(), varname_len)) {
        varname_len++;
        EatToken();
      }
      char* varname = strndup(&input_[varname_start], varname_len);
      bool braces_ok = true;
      if(has_braces && ((!EatToken()) || cur_tok_ != '}')) {
        braces_ok = false;
      }
      bool ok = varname_len && braces_ok;
      if (ok) {
        // first lookup the variable in our stored table
        const char* val = nullptr;
        auto iter = variables_->find(varname);
        if (iter != variables_->end()) {
          val = iter->second.c_str();
        } else {
          val = getenv(varname);
        }
        // if that fails, then check for a similar environment variable
        if (val) {
          output_ << val;
        } else {
          ok = false;
        }
      }
      if (!ok) {
        output_.write(&input_[start - 1], pos_ - start + 1);
      }
      free(varname);
      return ok;
    }

    static bool IsValidVariableCharacter(char c, int pos) {
      const char* valid_start = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
      const char* valid_follow = "1234567890";
      return (strchr(valid_start, c) != NULL ||
          ((0 == pos) && (strchr(valid_follow, c) != NULL)));
    }

    std::string input_;
    int pos_;
    char cur_tok_;
    std::stringstream output_;
    const StringStringMap* variables_;
};

std::string ExpandVariables(const std::string& input,
    const StringStringMap& vars) {
  return VariableExpander(input, vars).Result();
}

class ArgSeparator {
  public:

  private:
};

std::vector<std::string> SeparateArgs(const std::string& input) {
  // TODO don't use g_shell_parse_argv... it's not good with escape characters
  char** argv = NULL;
  int argc = -1;
  GError *err = NULL;
  gboolean parsed = g_shell_parse_argv(input.c_str(), &argc, &argv, &err);

  if(!parsed || err) {
    // unable to parse the command string as a Bourne shell command.
    // Do the simple thing and split it on spaces.
    std::vector<std::string> args = Split(input, " \t\n", 0);
    args.erase(std::remove_if(args.begin(), args.end(),
          [](const std::string& v) { return v.empty(); }), args.end());
    return args;
  }

  std::vector<std::string> result(argc);
  for (int i = 0; i < argc; ++i) {
    result[i] = argv[0];
  }
  Strfreev(argv);
  return result;
}

}  // namespace procman
