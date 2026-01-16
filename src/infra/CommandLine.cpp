#include "src/infra/CommandLine.hpp"
#include "thirdparty/hopscotch-map/hopscotch_map.h"
#include "thirdparty/ms-stl/charconv"
#include <algorithm>
#include <format>
#include <ostream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace dqsim::infra {
//---------------------------------------------------------------------------
namespace commandlineparser {
//---------------------------------------------------------------------------
void DefaultParserBase::error(ostream& out, string_view arg) {
   out << std::format("Invalid numerical value '{}'", arg) << '\n';
}
//---------------------------------------------------------------------------
bool DefaultParser<string>::parse(ostream& /*out*/, string& value, const string* arg) {
   if (arg)
      value = *arg;
   else
      value = {};
   return true;
}
//---------------------------------------------------------------------------
bool DefaultParser<bool>::parse(ostream& out, bool& value, const string* arg) {
   if (arg) {
      auto& a = *arg;
      if ((a == "y"sv) || (a == "yes"sv) || (a == "t"sv) || (a == "true"sv) || (a == "on"sv) || (a == "1"sv)) {
         value = true;
      } else if ((a == "n"sv) || (a == "no"sv) || (a == "f"sv) || (a == "false"sv) || (a == "off"sv) || (a == "0"sv)) {
         value = false;
      } else {
         out << std::format("Invalid boolean value '{}'\n", a);
         return false;
      }
   } else {
      value = true;
   }
   return true;
}
//---------------------------------------------------------------------------
bool DefaultParser<double>::parse(ostream& out, double& value, const string* arg) {
   if (arg) {
      auto [ptr, ec] = msstl::from_chars(arg->data(), arg->data() + arg->size(), value);
      if ((ec != std::errc()) || (ptr != (arg->data() + arg->size()))) {
         error(out, *arg);
         return false;
      }
   } else {
      value = {}; // currently unreachable, could change in the future
   }
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
CommandLine::OptionBase::OptionBase(bool argRequired)
   : argRequired(argRequired)
// Constructor
{
}
//---------------------------------------------------------------------------
CommandLine::OptionBase::~OptionBase()
// Destructor
{
}
//---------------------------------------------------------------------------
CommandLine::CommandLine(string helpIntro, string helpPattern, string helpFooter)
   : helpIntro(move(helpIntro)), helpPattern(move(helpPattern)), helpFooter(move(helpFooter))
// Constructor
{
}
//---------------------------------------------------------------------------
CommandLine::~CommandLine()
// Destructor
{
}
//---------------------------------------------------------------------------
CommandLine::OptionSpec CommandLine::add(OptionBase& option)
// Add an option
{
   options.push_back(&option);
   return OptionSpec(option);
}
//---------------------------------------------------------------------------
class CommandLine::Impl {
   public:
   static bool isLongOption(const string& arg, const tsl::hopscotch_map<string, CommandLine::OptionBase*>& longOptions);
   static void splitLongOption(const string& arg, string& key, string& value);
};
//---------------------------------------------------------------------------
bool CommandLine::Impl::isLongOption(const string& arg, const tsl::hopscotch_map<string, CommandLine::OptionBase*>& longOptions)
// Is a long option?
{
   // Multiple dashes are always long options
   if (arg[1] == '-')
      return true;

   // Assignment are also restricted to long options
   if (arg.find('=') != string::npos)
      return true;

   // Check if we know the name
   unsigned index = 0;
   for (unsigned limit = arg.length(); (index < limit) && (arg[index] == '-'); ++index) {}
   return longOptions.count(arg.substr(index));
}
//---------------------------------------------------------------------------
void CommandLine::Impl::splitLongOption(const string& arg, string& key, string& value)
// Split a long option into a key/value pair
{
   unsigned index = 0, limit = arg.length();
   for (; (index < limit) && (arg[index] == '-'); ++index) {}

   unsigned begin = index;
   for (; index < limit; ++index)
      if (arg[index] == '=') {
         key = arg.substr(begin, index - begin);
         value = arg.substr(index + 1);
         return;
      }

   key = arg.substr(begin);
   value = {};
}
//---------------------------------------------------------------------------
bool CommandLine::parse(std::ostream& out, int argc, char** argv)
// Parse the command line options
{
   if (argc > 0)
      executable = argv[0];

   // Build lookup tables
   tsl::hopscotch_map<string, OptionBase*> longOptions;
   tsl::hopscotch_map<char, OptionBase*> shortOptions;
   positional.clear();
   for (auto o : options) {
      o->seen = false;
      if (!o->longName.empty())
         longOptions[o->longName] = o;
      if (!!o->shortName)
         shortOptions[o->shortName] = o;
   }

   // Scan the arguments
   bool forcePositional = false;
   for (int index = 1; index < argc; ++index) {
      // Detect options
      if ((!forcePositional) && (argv[index][0] == '-') && (argv[index][1])) {
         string arg = argv[index];
         // Detect '--' to force positional arguments
         if ((arg.length() == 2) && (arg[1] == '-')) {
            forcePositional = true;
            continue;
         }

         // Landing pad for errors
         // LCOV_EXCL_START coverage is confused by the gotos here
         if (false) {
         reportError:
            out << std::format("Unknown option '{}'. Try {} --help", arg, executable) << '\n';
            return false;
         }
         if (false) {
         missingArg:
            out << std::format("Missing argument for option '{}'. Try {} --help", arg, executable) << '\n';
            return false;
         }
         // LCOV_EXCL_STOP

         // A long option?
         if (Impl::isLongOption(arg, longOptions)) {
            string key, value;
            Impl::splitLongOption(arg, key, value);

            auto iter = longOptions.find(key);
            if (iter == longOptions.end()) goto reportError;
            auto o = iter->second;

            if (o->seen) {
               out << std::format("Option '{}' specified multiple times.", std::string(arg)) << '\n';
               return false;
            }
            o->seen = true;

            // Do we have an argument?
            if (arg.find('=') != string::npos) {
               if (!o->parse(out, *this, &value))
                  return false;
            } else if (o->argRequired) {
               if (index + 1 >= argc) goto missingArg;
               value = argv[++index];
               if (!o->parse(out, *this, &value))
                  return false;
            } else {
               if (!o->parse(out, *this, nullptr))
                  return false; // currently unreachable
            }
            continue;
         }

         // Must be one (or more) short options
         unsigned index2 = 0, limit2 = arg.size();
         while ((index2 < limit2) && (arg[index2] == '-')) ++index2;
         if (index2 >= limit2) goto reportError;
         char duplicate = 0;
         for (; index2 < limit2; ++index2) {
            auto iter = shortOptions.find(arg[index2]);
            if (iter == shortOptions.end()) goto reportError;
            auto o = iter->second;
            if (o->seen && (!duplicate)) duplicate = arg[index2];
            o->seen = true;
            if (o->argRequired) {
               string value;
               if (index2 + 1 < limit2) {
                  value = arg.substr(index2 + 1);
               } else {
                  if (index + 1 >= argc) goto missingArg;
                  value = argv[++index];
               }
               if (!o->parse(out, *this, &value))
                  return false;
               break;
            } else {
               if (!o->parse(out, *this, nullptr))
                  return false; // currently unreachable
            }
         }
         if (duplicate) {
            out << std::format("Option '{}' specified multiple times.", duplicate) << '\n';
            return false;
         }
         continue;
      }

      // A position argument
      positional.emplace_back(argv[index]);
   }
   return true;
}
//---------------------------------------------------------------------------
void CommandLine::showHelp(std::ostream& out) const
// Show a help screen
{
   out << helpIntro << '\n';
   if (!helpPattern.empty()) {
      out << '\n'
          << std::format("Usage: {} {}", executable, helpPattern) << '\n';
   }

   vector<pair<string, string>> text;
   unsigned maxLen = 0;
   for (auto& o : options) {
      if (o->hidden) continue;
      string key;
      if (!!o->shortName) {
         char buf[3] = {'-', o->shortName, 0};
         key = buf;
         if (o->argRequired) key += " <value>";
      } else if (!o->longName.empty()) {
         key = "-"s + o->longName;
         if (o->argRequired) key += "=<value>";
      } else
         continue;
      if (key.length() > maxLen) maxLen = key.length();
      text.emplace_back(move(key), o->description);
   }
   if (text.empty()) return;
   for (auto& t : text)
      t.first.resize(maxLen, ' ');
   sort(text.begin(), text.end());

   out << '\n'
       << "Options:" << '\n';
   for (auto& t : text)
      out << t.first << " "sv << t.second << '\n';

   if (!helpFooter.empty()) {
      out << '\n'
          << helpFooter << '\n';
   }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
