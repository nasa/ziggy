
#include <iostream>
#include <sstream>
#include <fstream>

#include <unistd.h>

using namespace std;

int main( int argc, char**argv ){

  int retcode = 0;
  int sleepTime = 0;
  int crash = 0;
  int touch = 0;

  cout << "USAGE: testprog [retcode] [sleeptime] [crash (0/1)] [touch (0/1)]" << endl;
  cout << "testprog, argc = " << argc << endl;

  for( int i = 0; i < argc; i++ ){
    cout << "argv[" << i << "] = '" << argv[i] << "'" << endl;
  }

  // retcode
  if( argc >= 2 ){
    istringstream is( argv[1] );
    is >> retcode;
  }

  // sleeptime
  if( argc >= 3 ){
    istringstream is( argv[2] );
    is >> sleepTime;
  }

  if( argc >= 4 ){
    istringstream is( argv[3] );
    is >> crash;
  }
  
   if( argc >= 5 ){
    istringstream is( argv[4] );
    is >> touch;
  }
 

  cout << "retcode = " << retcode << endl;
  cout << "sleepTime = " << sleepTime << endl;
  cout << "crash = " << crash << endl;
  cout << "touch = " << touch << endl;

  cerr << "Here is some error stream content" << endl;
  cerr << "Here is some more error stream content" << endl;
  
  if( touch ) {
    ofstream ofs;
    ofs.open("touch.txt");
    ofs << "Here's some touch.txt content" << endl;
    ofs.close();
  }

  if( sleepTime > 0 ){
    sleep( sleepTime );
  }

  if( crash ){
    throw exception();
  }
  

  return retcode;
}

