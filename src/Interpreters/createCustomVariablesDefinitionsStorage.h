#pragma once

#include <memory>

namespace DB
{

class CustomVariablesDefinitionsDiskStorage;
class Context;
using ContextMutablePtr = std::shared_ptr<Context>;

std::unique_ptr<CustomVariablesDefinitionsDiskStorage> createCustomVariablesDefinitionsStorage(const ContextMutablePtr & global_context);

}
