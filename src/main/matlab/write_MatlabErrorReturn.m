function write_MatlabErrorReturn(file, s)

if(isnumeric(file))
    % assume a file descriptor was passed in
   fid = file;
   closeFile = false;
else
    % assume a filename was passed in
    fid = fopen(file, 'wb');
    if(fid == -1)
      error(['write_MatlabErrorReturn: Unable to open file: ' file ]);
    end;
    closeFile = true;
 end;

% String message
fwrite(fid, length(s.message), 'int32');
fwrite(fid, s.message, 'char');

% String identifier
fwrite(fid, length(s.identifier), 'int32');
fwrite(fid, s.identifier, 'char');

% MatlabStack stack[]
if(length(find(size(s.stack) > 1)) > 1)
	error('Error serializing .bin file: field s.stack is not a vector (see array2D_to_struct.m)');
end;
v1_stack_length = length(s.stack);
fwrite(fid, v1_stack_length, 'int32');

for i = 1:v1_stack_length
    write_MatlabStack(fid, s.stack(i));
end;

if(closeFile)
    fclose(fid);
end;

