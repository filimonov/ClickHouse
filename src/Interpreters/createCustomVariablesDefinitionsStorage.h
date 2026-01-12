#pragma once

#include <memory>

namespace DB
{

class ICustomVariablesDefinitionsStorage;
class Context;
using ContextMutablePtr = std::shared_ptr<Context>;

std::unique_ptr<ICustomVariablesDefinitionsStorage> createCustomVariablesDefinitionsStorage(const ContextMutablePtr & global_context);

}
