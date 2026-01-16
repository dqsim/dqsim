#pragma once
//---------------------------------------------------------------------------
#include "src/infra/Config.hpp"
#include <charconv>
#include <iosfwd>
#include <string>
#include <vector>
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
namespace commandlineparser {
//---------------------------------------------------------------------------
struct DefaultParserBase {
   protected:
   static void error(std::ostream& out, std::string_view arg);
};
//---------------------------------------------------------------------------
template <class T>
struct DefaultParser {};
template <>
struct DefaultParser<std::string> : public DefaultParserBase {
   static bool parse(std::ostream& out, std::string& value, const std::string* arg);
   static constexpr bool argRequired = true;
};
template <>
struct DefaultParser<bool> : public DefaultParserBase {
   static bool parse(std::ostream& out, bool& value, const std::string* arg);
   static constexpr bool argRequired = false;
};
template <>
struct DefaultParser<double> : public DefaultParserBase {
   static bool parse(std::ostream& out, double& value, const std::string* arg);
   static constexpr bool argRequired = true;
};
template <class T>
   requires(std::is_arithmetic_v<T>)
struct DefaultParser<T> : public DefaultParserBase {
   static bool parse(std::ostream& out, T& value, const std::string* arg) {
      if (arg) {
         auto [ptr, ec] = std::from_chars(arg->data(), arg->data() + arg->size(), value);
         if ((ec != std::errc()) || (ptr != (arg->data() + arg->size()))) {
            error(out, *arg);
            return false;
         }
      } else {
         value = {}; // currently unreachable, could change in the future
      }
      return true;
   }
   static constexpr bool argRequired = true;
};
//---------------------------------------------------------------------------
};
//---------------------------------------------------------------------------
/// A simple command line parser
class CommandLine {
   public:
   class Impl;
   class OptionSpec;
   /// Base class for options
   class OptionBase {
      /// Long name (if any)
      std::string longName;
      /// Description
      std::string description;
      /// Short name (if any)
      char shortName = 0;
      /// Argument required?
      bool argRequired;
      /// Has a default value?
      bool defaultValue = false;
      /// A hidden option?
      bool hidden = false;
      /// Was specified?
      bool seen = false;

      /// Parse an argument
      virtual bool parse(std::ostream& out, CommandLine& cl, const std::string* arg) = 0;

      friend class CommandLine;
      friend class CommandLine::Impl;
      friend class OptionSpec;

      protected:
      /// Constructor
      explicit OptionBase(bool argRequired);
      /// Destructor
      ~OptionBase();

      public:
      /// Not set?
      bool operator!() const { return (!seen) && (!defaultValue); }
   };
   /// Helper to specify options
   class OptionSpec {
      /// The option
      OptionBase& option;

      /// Constructor
      explicit OptionSpec(OptionBase& option) : option(option) {}

      friend class CommandLine;

      public:
      /// Set the short name
      OptionSpec& shortName(char n) {
         option.shortName = n;
         return *this;
      }
      /// Set the long name
      OptionSpec& longName(std::string n) {
         option.longName = std::move(n);
         return *this;
      }
      /// Set the description
      OptionSpec& description(std::string n) {
         option.description = std::move(n);
         return *this;
      }
      /// Mark as hidden
      OptionSpec& hidden() {
         option.hidden = true;
         return *this;
      }
      /// Mark as having a default value
      OptionSpec& defaultValue() {
         option.defaultValue = true;
         return *this;
      }
   };
   /// An option
   template <class T, class Parser = commandlineparser::DefaultParser<T>>
   class Option : public OptionBase {
      /// The value
      T value;

      /// Parse an argument
      bool parse(std::ostream& out, CommandLine& /*cl*/, const std::string* arg) override { return Parser::parse(out, value, arg); }

      public:
      /// Constructor
      explicit Option(T value = {}) : OptionBase(Parser::argRequired), value(std::move(value)) {}

      /// Get the value
      const T& get() const { return value; }
   };

   private:
   /// The help text
   std::string helpIntro, helpPattern, helpFooter;
   /// All options
   std::vector<OptionBase*> options;
   /// The executable name
   std::string executable;
   /// The positional arguments
   std::vector<std::string> positional;

   protected:
   /// Add an option
   OptionSpec add(OptionBase& option);

   public:
   /// Constructor
   CommandLine(std::string helpIntro, std::string helpPattern = {}, std::string helpFooter = {});
   /// Destructor
   ~CommandLine();

   /// Parse the command line options
   bool parse(std::ostream& out, int argc, char** argv);
   /// Show a help screen
   void showHelp(std::ostream& out) const;
   /// Get the positional arguments
   auto& getPositional() const { return positional; }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
