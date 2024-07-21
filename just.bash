export JUST_PROJECT_ROOT="`pwd`";

echo "NOTE: SOURCE THIS FILE WHILE INSIDE THE SAME PATH AS THIS FILE. ELSE YOU WILL HAVE INVALID BEHAVIOR";

function run {
  cd "$JUST_PROJECT_ROOT";
  dotnet run;
  cd ~-;
}

function build {
  cd "$JUST_PROJECT_ROOT";
  dotnet build;
  cd ~-;
}

function test {
  cd "$JUST_PROJECT_ROOT";
  dotnet test;
  cd ~-;
}
