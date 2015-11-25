#include "exec_string_utils.hpp"

#include <sstream>

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
      while(NextChar()) {
        const char c = cur_char_;
        if('\\' == c) {
          if(NextChar()) {
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

    bool HasChar() const {
      return pos_ < input_.size();
    }

    char PeekChar() const {
      return HasChar() ? input_[pos_] : 0;
    }

    bool NextChar() {
      if(HasChar()) {
        cur_char_ = input_[pos_];
        pos_++;
        return true;
      } else {
        cur_char_ = 0;
        return false;
      }
    }

    bool ParseVariable() {
      int start = pos_;
      if(!HasChar()) {
        output_.put('$');
        return false;
      }
      int has_braces = PeekChar() == '{';
      if(has_braces) {
        NextChar();
      }
      int varname_start = pos_;
      int varname_len = 0;
      while(HasChar() &&
          IsValidVariableCharacter(PeekChar(), varname_len)) {
        varname_len++;
        NextChar();
      }
      char* varname = strndup(&input_[varname_start], varname_len);
      bool braces_ok = true;
      if(has_braces && ((!NextChar()) || cur_char_ != '}')) {
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

    const std::string& input_;
    int pos_;
    char cur_char_;
    std::stringstream output_;
    const StringStringMap* variables_;
};

std::string ExpandVariables(const std::string& input,
    const StringStringMap& vars) {
  return VariableExpander(input, vars).Result();
}

class ArgSeparator {
  public:
    ArgSeparator(const std::string& input) :
      input_(input),
      pos_(0) {
      Process();
    }

    std::vector<std::string> Result() {
      return result_;
    }

  private:
    void Process() {
      while (ParseArg()) {
        result_.emplace_back(&cur_arg_[0], cur_arg_.size());
      }
    }

    bool ParseArg() {
      cur_arg_.clear();

      // Skip leading whitespace
      while (NextChar() && std::isspace(cur_char_)) {}
      if (!HasChar()) {
        return false;
      }

      bool quoted = false;
      char quote_char = 0;
      do {
        if (cur_char_ == '\\') {
          cur_arg_.push_back(NextChar() ? cur_char_ : '\\');
        } else if (!quoted && cur_char_ == '\'') {
          quoted = true;
          quote_char = '\'';
        } else if (!quoted && cur_char_ == '"') {
          quoted = true;
          quote_char = '"';
        } else if (quoted && quote_char == cur_char_) {
          quoted = false;
        } else if (!quoted && std::isspace(cur_char_)) {
          return true;
        } else {
          cur_arg_.push_back(cur_char_);
        }
      } while (NextChar());
      return true;
    }

    bool HasChar() const {
      return pos_ < input_.size();
    }

    bool NextChar() {
      if(HasChar()) {
        cur_char_ = input_[pos_];
        pos_++;
        return true;
      } else {
        cur_char_ = 0;
        return false;
      }
    }

    const std::string& input_;
    int pos_;
    char cur_char_;
    std::vector<std::string> result_;
    std::vector<char> cur_arg_;
};

std::vector<std::string> SeparateArgs(const std::string& input) {
  return ArgSeparator(input).Result();
}

}  // namespace procman
